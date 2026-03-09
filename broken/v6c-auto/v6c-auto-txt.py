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

    return cfg


def fmt_hhmmss(seconds: float) -> str:
    if seconds < 0:
        seconds = 0
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h}:{m:02d}:{sec:02d}"


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
    # dedupe, preserve order
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
    """
    Vraci (workers_target, concurrent_fragments) podle prumerne rychlosti.
    Je to heuristika, ne magie.
    """
    if mbps <= 0:
        return (6, 3)

    # workers_target: spis opatrne, aby se nevyvolal throttling
    if mbps < 30:
        wt = 3
    elif mbps < 80:
        wt = 5
    elif mbps < 150:
        wt = 7
    else:
        wt = 10

    # fragments: pro HLS/DASH casto pomaha zvednout
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

    # ETA health heuristic
    last_eta: int = -1
    last_eta_check_at: float = 0.0
    eta_bad: bool = False

    # probe info
    probe_mbps: float = 0.0


class AdjustableSemaphore:
    """
    Semafor s menitelnym cilem.
    Zvyseni je snadne (release).
    Snizeni: jen nastavi target, nove acquire budou blokovat, bezici joby dobehnou.
    """
    def __init__(self, initial: int):
        self._sem = threading.Semaphore(initial)
        self._lock = threading.Lock()
        self._target = initial
        self._max = initial

    def acquire(self):
        self._sem.acquire()

    def release(self):
        self._sem.release()

    def set_target(self, new_target: int):
        if new_target < 1:
            new_target = 1
        with self._lock:
            cur = self._target
            self._target = new_target
            # if raising target, release difference immediately
            if new_target > cur:
                for _ in range(new_target - cur):
                    self._sem.release()

    @property
    def target(self) -> int:
        with self._lock:
            return self._target


class GrabberDaemonV6:
    def __init__(self, cfg: dict, script_dir: Path):
        self.cfg = cfg
        self.script_dir = script_dir

        self.out_dir = Path(cfg["download_dir"]).expanduser().resolve()
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self.job_q: queue.Queue[Job] = queue.Queue()
        self.jobs: dict[str, Job] = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        self.pending = deque()  # URLs in memory not yet enqueued into job_q
        self.queue_limit = int(cfg["queue_limit"])

        # adjustable concurrency limit (workers_target)
        self.active_gate = AdjustableSemaphore(int(cfg["workers_target"]))

        # csv mode autoload
        self.csv_path = find_first_csv_in_dir(script_dir)
        if self.csv_path:
            urls = parse_urls_from_csv(self.csv_path)
            for u in urls:
                self.pending.append(u)

        # feeder thread keeps job_q filled up to queue_limit
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
        # user-added always goes to end (especially in CSV mode)
        with self.lock:
            self.pending.append(url)

    def _feeder_loop(self):
        while not self.stop_event.is_set():
            try:
                with self.lock:
                    # keep job_q size under limit
                    qsize = self.job_q.qsize()
                    while qsize < self.queue_limit and self.pending:
                        url = self.pending.popleft()
                        job_id = self._new_job_id()
                        job = Job(job_id=job_id, url=url, status="queued")
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

        active = sum(1 for j in jobs if j.status in ("downloading", "probing"))
        done = sum(1 for j in jobs if j.status == "done")
        fail = sum(1 for j in jobs if j.status == "fail")
        queued = sum(1 for j in jobs if j.status == "queued")

        print("Video Grabber v6c-auto-txt (paste URL + Enter). Ctrl+C to quit.\n")
        print(f"CSV: {csv_info}")
        print(f"Output: {self.out_dir}")
        print(
            f"Workers(max): {self.cfg['max_workers']} | Workers(target): {self.active_gate.target} | "
            f"Frags: {self.cfg['concurrent_fragments']} | Queue(limit): {self.queue_limit} | "
            f"Queue(now): {qsize} | Pending: {pending_count}"
        )
        print(f"queued={queued} active={active} done={done} fail={fail}\n")

        now = time.time()
        jobs_sorted = sorted(jobs, key=lambda j: (j.status != "downloading", j.status != "probing", j.status != "queued", j.job_id))

        for j in jobs_sorted[:25]:
            jid = c(j.job_id, BLUE)

            if j.status == "probing":
                # show probe in grey/yellow
                probe_txt = f"{j.probe_mbps:.1f} Mbps" if j.probe_mbps > 0 else "..."
                print(f"{jid} {c('[PROBING]', DIM)} {c(probe_txt, YELLOW)} {c(j.url, DIM)}")

            elif j.status == "downloading":
                bar = c(self._bar(j.downloaded, j.total), YELLOW)
                pct = f"{(j.downloaded / j.total * 100):5.1f}%" if j.total else "  ?.?%"
                sp = c(f"{(j.speed / 1024 / 1024):5.1f}MB/s", GREEN) if j.speed else c("  ?.?MB/s", GREEN)

                eta_txt = fmt_hhmmss(j.eta)
                elap_txt = fmt_hhmmss((now - j.started_at) if j.started_at else 0)

                eta_color = RED if j.eta_bad else CYAN
                eta_part = c("ETA ", DIM) + c(eta_txt, eta_color)
                elap_part = c("ELAP ", DIM) + elap_txt  # ELAP value white

                probe_hint = f"  {c('PROBE', DIM)} {c(f'{j.probe_mbps:.1f}Mbps', DIM)}" if j.probe_mbps > 0 else ""
                print(f"{jid} {bar} {pct} {sp} {eta_part} {elap_part}{probe_hint}")

            elif j.status == "queued":
                print(f"{jid} {c('[QUEUED]', DIM)} {c(j.url, DIM)}")

            elif j.status == "done":
                elap_txt = fmt_hhmmss((j.finished_at - j.started_at) if (j.started_at and j.finished_at) else 0)
                saved = j.final_path or "(path unknown)"
                print(f"{jid} {c('[DONE]', DIM)} in {elap_txt} -> {c(saved, DIM)}")

            elif j.status == "fail":
                err = (j.error[:140] + "…") if len(j.error) > 140 else j.error
                print(f"{jid} {c('[FAIL]', RED)} {err}")

        # show queue preview in grey
        print("\n" + c("Queue preview (pending, in memory):", DIM))
        if next_pending:
            for u in next_pending:
                print(c("  - " + u, DIM))
            if pending_count > len(next_pending):
                print(c(f"  ... +{pending_count - len(next_pending)} dalsich", DIM))
        else:
            print(c("  (empty)", DIM))

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

                    # ETA heuristic (as you asked: leave it)
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

    def _probe_speed_on_video(self, url: str) -> float:
        """
        Stahne zacatek videa do docasne slozky a soubory zahodi.
        Snazi se "nahrabat" ~probe_target_mb (v MB) pomoci zacinajicich useku (download_sections).
        Vraci prumerne Mbps (megabits/sec). Kdyz probe selze, vrati 0.
        """
        if not self.cfg.get("auto_tune", True):
            return 0.0

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

        # simple loop: 10s -> 20s -> 30s ... until target or max
        sec = first_s
        try:
            while sec <= max_s and (got_bytes < target_mb * 1024 * 1024):
                section = f"*0-{sec}"
                # progress hook to count bytes
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

                    # NO post-processing for probe (rychlejsi); jen stahnout zacatek
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

                sec += first_s  # step up

            dt = max(0.001, time.time() - start)
            mbps = (got_bytes * 8) / dt / 1_000_000
            return mbps if mbps > 0 else 0.0

        except Exception:
            return 0.0

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

            # gate: realny pocet aktivnich downloadu (workers_target)
            self.active_gate.acquire()

            try:
                with self.lock:
                    job.status = "probing"
                    job.started_at = 0.0  # start "real" timer az pri downloadu

                # --- probe (discarded) ---
                mbps = self._probe_speed_on_video(job.url)
                with self.lock:
                    job.probe_mbps = mbps

                # tune global values based on this probe
                if self.cfg.get("auto_tune", True) and mbps > 0:
                    wt, fr = choose_tuned_values(mbps)
                    # apply: concurrency gate and fragments
                    self.active_gate.set_target(wt)
                    with self.lock:
                        self.cfg["concurrent_fragments"] = fr

                # --- real download ---
                with self.lock:
                    job.status = "downloading"
                    if job.started_at == 0.0:
                        job.started_at = time.time()

                opts = self._build_ydl_opts(job)
                with YoutubeDL(opts) as ydl:
                    ydl.extract_info(job.url, download=True)

                # prefer mp4 if exists
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
        # start workers (threads) up to max_workers
        for _ in range(int(self.cfg["max_workers"])):
            t = threading.Thread(target=self.worker_loop, daemon=True)
            t.start()

        tmon = threading.Thread(target=self.monitor_loop, daemon=True)
        tmon.start()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()

    script_dir = Path(__file__).resolve().parent
    cfg = load_config(args.config)

    daemon = GrabberDaemonV6(cfg, script_dir)
    daemon.start()

    try:
        while True:
            line = input().strip()
            if not line:
                continue
            # take first token (URL)
            candidate = line.split()[0].strip()
            if not URL_RE.match(candidate):
                continue
            daemon.add_url_user(candidate)
    except KeyboardInterrupt:
        daemon.stop_event.set()
        print("\nExiting...")


if __name__ == "__main__":
    main()
