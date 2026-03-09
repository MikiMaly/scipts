import argparse
import csv
import os
import queue
import re
import shutil
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import tempfile
from urllib.parse import urlparse

import yaml
from yt_dlp import YoutubeDL

# Colors (Windows-friendly)
try:
    from colorama import init as colorama_init
    colorama_init()
    COLOR = True
except Exception:
    COLOR = False


def c(text: str, code: str) -> str:
    if not COLOR:
        return text
    return f"\x1b[{code}m{text}\x1b[0m"


BLUE = "34"
YELLOW = "33"
GREEN = "32"
RED = "31"
CYAN = "36"
DIM = "2"

URL_RE = re.compile(r"https?://[^\s,;\"']+", re.IGNORECASE)


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    cfg.setdefault("download_dir", "./downloads")
    cfg.setdefault("max_workers", 10)
    cfg.setdefault("workers_target", cfg["max_workers"])
    cfg.setdefault("timeout_sec", 60)

    cfg.setdefault("format", "bv*+ba/b")
    cfg.setdefault("retries", 20)
    cfg.setdefault("fragment_retries", None)
    cfg.setdefault("concurrent_fragments", 3)

    cfg.setdefault("user_agent", "Mozilla/5.0")
    cfg.setdefault("ffmpeg_path", "")

    # v6c-auto-txt
    cfg.setdefault("queue_limit", 30)
    cfg.setdefault("auto_tune", True)
    cfg.setdefault("probe_target_mb", 25)
    cfg.setdefault("probe_first_seconds", 10)
    cfg.setdefault("probe_max_seconds", 60)
    cfg.setdefault("probe_cache_minutes", 30)

    return cfg


def fmt_hhmmss(seconds: float) -> str:
    if seconds < 0:
        seconds = 0
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h}:{m:02d}:{sec:02d}"


def get_domain(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        return host.lower()
    except Exception:
        return ""


def parse_urls_from_csv(csv_path: Path) -> list[str]:
    urls = []
    try:
        with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                for cell in row:
                    if not cell:
                        continue
                    for m in URL_RE.findall(cell):
                        urls.append(m.strip())
    except Exception:
        return []

    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def find_first_csv_in_dir(dir_path: Path) -> Path | None:
    for p in sorted(dir_path.glob("*.csv")):
        if p.is_file():
            return p
    return None


def choose_tuned_values(mbps: float) -> tuple[int, int]:
    if mbps <= 0:
        return (6, 3)

    if mbps < 30:
        wt = 3
    elif mbps < 80:
        wt = 5
    elif mbps < 150:
        wt = 7
    else:
        wt = 10

    if mbps < 30:
        fr = 2
    elif mbps < 80:
        fr = 4
    elif mbps < 200:
        fr = 6
    else:
        fr = 8

    return (wt, fr)


@dataclass
class Job:
    job_id: str
    url: str
    status: str = "queued"  # queued/probing/downloading/done/fail

    downloaded: int = 0
    total: int = 0
    speed: float = 0.0
    eta: int = 0

    started_at: float = 0.0
    finished_at: float = 0.0

    error: str = ""
    final_path: str = ""

    last_eta: int = -1
    last_eta_check_at: float = 0.0
    eta_bad: bool = False

    probe_mbps: float = 0.0
    domain: str = ""


class AdjustableSemaphore:
    def __init__(self, initial: int):
        self._sem = threading.Semaphore(max(1, initial))
        self._lock = threading.Lock()
        self._target = max(1, initial)

    def acquire(self):
        self._sem.acquire()

    def release(self):
        self._sem.release()

    def set_target(self, new_target: int):
        new_target = max(1, int(new_target))
        with self._lock:
            cur = self._target
            self._target = new_target
            if new_target > cur:
                for _ in range(new_target - cur):
                    self._sem.release()

    @property
    def target(self) -> int:
        with self._lock:
            return self._target


class ProbeCache:
    """
    Cache probe vysledku per domena:
    domain -> (timestamp, mbps, workers_target, fragments)
    """
    def __init__(self, ttl_minutes: int):
        self.ttl = max(1, int(ttl_minutes)) * 60
        self.lock = threading.Lock()
        self.data: dict[str, tuple[float, float, int, int]] = {}

    def get(self, domain: str):
        if not domain:
            return None
        now = time.time()
        with self.lock:
            v = self.data.get(domain)
            if not v:
                return None
            ts, mbps, wt, fr = v
            if now - ts > self.ttl:
                self.data.pop(domain, None)
                return None
            return v

    def set(self, domain: str, mbps: float, wt: int, fr: int):
        if not domain:
            return
        with self.lock:
            self.data[domain] = (time.time(), float(mbps), int(wt), int(fr))


class GrabberDaemonV6Cache:
    def __init__(self, cfg: dict, script_dir: Path):
        self.cfg = cfg
        self.script_dir = script_dir

        self.out_dir = Path(cfg["download_dir"]).expanduser().resolve()
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self.job_q: queue.Queue[Job] = queue.Queue()
        self.jobs: dict[str, Job] = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        self.pending = deque()
        self.queue_limit = int(cfg["queue_limit"])

        self.active_gate = AdjustableSemaphore(int(cfg["workers_target"]))

        # probe cache
        self.probe_cache = ProbeCache(int(cfg.get("probe_cache_minutes", 30)))

        # csv mode autoload
        self.csv_path = find_first_csv_in_dir(script_dir)
        if self.csv_path:
            urls = parse_urls_from_csv(self.csv_path)
            for u in urls:
                self.pending.append(u)

        self.feeder_thread = threading.Thread(target=self._feeder_loop, daemon=True)
        self.feeder_thread.start()

    def _new_job_id(self) -> str:
        base = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_id = base
        i = 2
        with self.lock:
            while job_id in self.jobs:
                job_id = f"{base}_{i}"
                i += 1
        return job_id

    def add_url_user(self, url: str):
        url = url.strip()
        if not url:
            return
        with self.lock:
            self.pending.append(url)

    def _feeder_loop(self):
        while not self.stop_event.is_set():
            try:
                with self.lock:
                    qsize = self.job_q.qsize()
                    while qsize < self.queue_limit and self.pending:
                        url = self.pending.popleft()
                        job_id = self._new_job_id()
                        job = Job(job_id=job_id, url=url, status="queued", domain=get_domain(url))
                        self.jobs[job_id] = job
                        self.job_q.put(job)
                        qsize += 1
                time.sleep(0.2)
            except Exception:
                time.sleep(0.5)

    def _bar(self, downloaded: int, total: int, width: int = 24) -> str:
        if total <= 0:
            return "[" + ("?" * width) + "]"
        pct = max(0.0, min(1.0, downloaded / total))
        filled = int(width * pct)
        return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"

    def _render(self):
        os.system("cls")
        with self.lock:
            jobs = list(self.jobs.values())
            qsize = self.job_q.qsize()
            pending_count = len(self.pending)
            next_pending = list(self.pending)[:8]
            csv_info = str(self.csv_path.name) if self.csv_path else "none"
            fr = int(self.cfg["concurrent_fragments"])

        active = sum(1 for j in jobs if j.status in ("downloading", "probing"))
        done = sum(1 for j in jobs if j.status == "done")
        fail = sum(1 for j in jobs if j.status == "fail")
        queued = sum(1 for j in jobs if j.status == "queued")

        print("Video Grabber v6c-auto-txt-cache (paste URL + Enter). Ctrl+C to quit.\n")
        print(f"CSV: {csv_info}")
        print(f"Output: {self.out_dir}")
        print(
            f"Workers(max): {self.cfg['max_workers']} | Workers(target): {self.active_gate.target} | "
            f"Frags: {fr} | Queue(limit): {self.queue_limit} | Queue(now): {qsize} | Pending: {pending_count}"
        )
        print(f"queued={queued} active={active} done={done} fail={fail}\n")

        now = time.time()
        jobs_sorted = sorted(
            jobs,
            key=lambda j: (j.status != "downloading", j.status != "probing", j.status != "queued", j.job_id)
        )

        for j in jobs_sorted[:25]:
            jid = c(j.job_id, BLUE)

            if j.status == "probing":
                probe_txt = f"{j.probe_mbps:.1f} Mbps" if j.probe_mbps > 0 else "..."
                dom = c(j.domain, DIM) if j.domain else ""
                print(f"{jid} {c('[PROBING]', DIM)} {c(probe_txt, YELLOW)} {dom} {c(j.url, DIM)}")

            elif j.status == "downloading":
                bar = c(self._bar(j.downloaded, j.total), YELLOW)
                pct = f"{(j.downloaded / j.total * 100):5.1f}%" if j.total else "  ?.?%"
                sp = c(f"{(j.speed / 1024 / 1024):5.1f}MB/s", GREEN) if j.speed else c("  ?.?MB/s", GREEN)

                eta_txt = fmt_hhmmss(j.eta)
                elap_txt = fmt_hhmmss((now - j.started_at) if j.started_at else 0)

                eta_color = RED if j.eta_bad else CYAN
                eta_part = c("ETA ", DIM) + c(eta_txt, eta_color)
                elap_part = c("ELAP ", DIM) + elap_txt

                probe_hint = f"  {c('PROBE', DIM)} {c(f'{j.probe_mbps:.1f}Mbps', DIM)}" if j.probe_mbps > 0 else ""
                print(f"{jid} {bar} {pct} {sp} {eta_part} {elap_part}{probe_hint}")

            elif j.status == "queued":
                print(f"{jid} {c('[QUEUED]', DIM)} {c(j.domain, DIM)} {c(j.url, DIM)}")

            elif j.status == "done":
                elap_txt = fmt_hhmmss((j.finished_at - j.started_at) if (j.started_at and j.finished_at) else 0)
                saved = j.final_path or "(path unknown)"
                print(f"{jid} {c('[DONE]', DIM)} in {elap_txt} -> {c(saved, DIM)}")

            elif j.status == "fail":
                err = (j.error[:140] + "…") if len(j.error) > 140 else j.error
                print(f"{jid} {c('[FAIL]', RED)} {err}")

        print("\n" + c("Queue preview (pending, in memory):", DIM))
        if next_pending:
            for u in next_pending:
                print(c("  - " + u, DIM))
            if pending_count > len(next_pending):
                print(c(f"  ... +{pending_count - len(next_pending)} dalsich", DIM))
        else:
            print(c("  (empty)", DIM))

        # Notes (the two important ones) – shown in UI
        print("\n" + c("Poznamky:", DIM))
        print(c("1) Probe je cachovany per-domena (hostname) a dela se max 1x za TTL; pro dalsi URL ze stejneho webu se probe preskoci.", DIM))
        print(c("2) Pokud probe selze / vrati 0, nastaveni se nemeni a pouzije se YAML (workers_target a concurrent_fragments).", DIM))

        print("\nPaste URL here and press Enter:")

    def _build_ydl_opts(self, job: Job) -> dict:
        outtmpl = str(self.out_dir / job.job_id) + ".%(ext)s"
        headers = {"User-Agent": self.cfg["user_agent"], "Referer": job.url}
        ffmpeg_loc = (self.cfg.get("ffmpeg_path") or "").strip()

        retries = int(self.cfg["retries"])
        frag_retries_cfg = self.cfg.get("fragment_retries")
        fragment_retries = int(frag_retries_cfg) if frag_retries_cfg is not None else retries

        def hook(d):
            with self.lock:
                j = self.jobs.get(job.job_id)
                if not j:
                    return

                st = d.get("status")
                if st == "downloading":
                    if j.started_at == 0.0:
                        j.started_at = time.time()
                    j.status = "downloading"
                    j.downloaded = int(d.get("downloaded_bytes") or 0)
                    j.total = int(d.get("total_bytes") or d.get("total_bytes_estimate") or 0)
                    j.speed = float(d.get("speed") or 0.0)
                    j.eta = int(d.get("eta") or 0)

                    # ETA heuristic (as-is)
                    now = time.time()
                    if j.last_eta_check_at == 0.0:
                        j.last_eta_check_at = now
                        j.last_eta = j.eta
                        j.eta_bad = False
                    elif now - j.last_eta_check_at >= 2.0 and j.last_eta >= 0 and j.eta >= 0:
                        dt = now - j.last_eta_check_at
                        expected_drop = max(1, int(dt))
                        actual_drop = j.last_eta - j.eta
                        j.eta_bad = actual_drop < max(1, expected_drop // 2)
                        j.last_eta_check_at = now
                        j.last_eta = j.eta

                elif st == "finished":
                    fn = d.get("filename")
                    if fn:
                        j.final_path = str(Path(fn).resolve())

                elif st == "error":
                    j.status = "fail"

        opts = {
            "format": self.cfg["format"],
            "merge_output_format": "mp4",
            "remuxvideo": "mp4",

            "outtmpl": outtmpl,
            "retries": retries,
            "fragment_retries": fragment_retries,
            "concurrent_fragment_downloads": int(self.cfg["concurrent_fragments"]),
            "socket_timeout": int(self.cfg["timeout_sec"]),
            "noplaylist": True,

            "quiet": True,
            "no_warnings": True,
            "noprogress": True,

            "http_headers": headers,
            "progress_hooks": [hook],
        }

        if ffmpeg_loc:
            opts["ffmpeg_location"] = ffmpeg_loc

        return opts

    def _probe_speed_on_video(self, url: str, domain: str) -> tuple[float, int, int]:
        """
        Probe zacatku videa (discard):
        - stahne cast od zacatku pres download_sections
        - snazi se nahrabat ~probe_target_mb
        - probe soubory smaze
        Vraci (mbps, workers_target, fragments). Kdyz probe selze: (0, 0, 0).
        """
        if not self.cfg.get("auto_tune", True):
            return (0.0, 0, 0)

        target_mb = int(self.cfg.get("probe_target_mb", 25))
        first_s = int(self.cfg.get("probe_first_seconds", 10))
        max_s = int(self.cfg.get("probe_max_seconds", 60))

        headers = {"User-Agent": self.cfg["user_agent"], "Referer": url}
        ffmpeg_loc = (self.cfg.get("ffmpeg_path") or "").strip()

        retries = int(self.cfg["retries"])
        frag_retries_cfg = self.cfg.get("fragment_retries")
        fragment_retries = int(frag_retries_cfg) if frag_retries_cfg is not None else retries

        tmpdir = Path(tempfile.mkdtemp(prefix="vgrab_probe_"))
        got_bytes = 0
        start = time.time()

        sec = first_s
        try:
            while sec <= max_s and (got_bytes < target_mb * 1024 * 1024):
                section = f"*0-{sec}"
                local_bytes = {"dl": 0}

                def ph(d):
                    if d.get("status") == "downloading":
                        b = int(d.get("downloaded_bytes") or 0)
                        if b > local_bytes["dl"]:
                            local_bytes["dl"] = b

                outtmpl = str(tmpdir / "probe") + ".%(ext)s"

                opts = {
                    "format": self.cfg["format"],
                    "outtmpl": outtmpl,

                    "download_sections": {"*": section},
                    "force_keyframes_at_cuts": False,

                    "retries": retries,
                    "fragment_retries": fragment_retries,
                    "concurrent_fragment_downloads": int(self.cfg["concurrent_fragments"]),
                    "socket_timeout": int(self.cfg["timeout_sec"]),
                    "noplaylist": True,

                    "quiet": True,
                    "no_warnings": True,
                    "noprogress": True,

                    "http_headers": headers,
                    "progress_hooks": [ph],
                }
                if ffmpeg_loc:
                    opts["ffmpeg_location"] = ffmpeg_loc

                with YoutubeDL(opts) as ydl:
                    ydl.extract_info(url, download=True)

                got_bytes = max(got_bytes, local_bytes["dl"])
                if got_bytes >= target_mb * 1024 * 1024:
                    break
                sec += first_s

            dt = max(0.001, time.time() - start)
            mbps = (got_bytes * 8) / dt / 1_000_000
            if mbps <= 0:
                return (0.0, 0, 0)

            wt, fr = choose_tuned_values(mbps)
            return (mbps, wt, fr)

        except Exception:
            return (0.0, 0, 0)

        finally:
            try:
                shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass

    def worker_loop(self):
        while not self.stop_event.is_set():
            try:
                job = self.job_q.get(timeout=0.2)
            except queue.Empty:
                continue

            self.active_gate.acquire()

            try:
                domain = job.domain or get_domain(job.url)

                # 1) Probe: use cache per domain
                cached = self.probe_cache.get(domain) if self.cfg.get("auto_tune", True) else None
                if cached:
                    ts, mbps, wt, fr = cached
                    with self.lock:
                        job.probe_mbps = mbps
                        job.status = "downloading"
                    # apply cached tuning
                    self.active_gate.set_target(wt)
                    with self.lock:
                        self.cfg["concurrent_fragments"] = fr
                else:
                    with self.lock:
                        job.status = "probing"
                    mbps, wt, fr = self._probe_speed_on_video(job.url, domain)
                    with self.lock:
                        job.probe_mbps = mbps

                    # apply only if probe succeeded
                    if self.cfg.get("auto_tune", True) and mbps > 0 and wt > 0 and fr > 0:
                        self.probe_cache.set(domain, mbps, wt, fr)
                        self.active_gate.set_target(wt)
                        with self.lock:
                            self.cfg["concurrent_fragments"] = fr
                    # if probe failed, do nothing (use YAML)

                # 2) Real download
                with self.lock:
                    job.status = "downloading"
                    if job.started_at == 0.0:
                        job.started_at = time.time()

                opts = self._build_ydl_opts(job)
                with YoutubeDL(opts) as ydl:
                    ydl.extract_info(job.url, download=True)

                if not job.final_path:
                    matches = sorted(
                        self.out_dir.glob(job.job_id + ".*"),
                        key=lambda p: p.stat().st_mtime,
                        reverse=True
                    )
                    if matches:
                        job.final_path = str(matches[0].resolve())

                mp4 = self.out_dir / f"{job.job_id}.mp4"
                if mp4.exists():
                    job.final_path = str(mp4.resolve())

                with self.lock:
                    job.status = "done"
                    job.finished_at = time.time()

            except Exception as e:
                with self.lock:
                    job.status = "fail"
                    job.finished_at = time.time()
                    job.error = str(e)

            finally:
                self.job_q.task_done()
                self.active_gate.release()

    def monitor_loop(self):
        while not self.stop_event.is_set():
            self._render()
            time.sleep(0.5)

    def start(self):
        for _ in range(int(self.cfg["max_workers"])):
            threading.Thread(target=self.worker_loop, daemon=True).start()
        threading.Thread(target=self.monitor_loop, daemon=True).start()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()

    script_dir = Path(__file__).resolve().parent
    cfg = load_config(args.config)

    daemon = GrabberDaemonV6Cache(cfg, script_dir)
    daemon.start()

    try:
        while True:
            line = input().strip()
            if not line:
                continue
            candidate = line.split()[0].strip()
            if not URL_RE.match(candidate):
                continue
            daemon.add_url_user(candidate)
    except KeyboardInterrupt:
        daemon.stop_event.set()
        print("\nExiting...")


if __name__ == "__main__":
    main()
