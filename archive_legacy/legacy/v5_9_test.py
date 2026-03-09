import argparse
import csv
import queue
import re
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml
from yt_dlp import YoutubeDL

# =========================
# v5.9 - Video Grabber
# queue_limit + pending + feeder
# timestamp + jid naming
# queued->downloading->done->fail ordering
# queued shows [QUEUED] + ENQ at far right
# header + prompt separators
# =========================

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
MAGENTA = "35"
DIM = "2"

CSI = "\x1b["
URL_FINDER = re.compile(r"https?://[^\s,;\"']+", re.IGNORECASE)


def ansi_clear_screen():
    print(CSI + "2J" + CSI + "H", end="")


def ansi_home():
    print(CSI + "H", end="")


def ansi_clear_to_end():
    print(CSI + "J", end="")


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    cfg.setdefault("download_dir", "./downloads")
    cfg.setdefault("max_workers", 10)
    cfg.setdefault("timeout_sec", 60)

    cfg.setdefault("format", "bv*+ba/b")
    cfg.setdefault("retries", 20)
    cfg.setdefault("fragment_retries", None)
    cfg.setdefault("concurrent_fragments", 3)

    cfg.setdefault("user_agent", "Mozilla/5.0")
    cfg.setdefault("ffmpeg_path", "")

    cfg.setdefault("refresh_sec", 0.6)

    # metadata prefetch (fills TOTAL for queued)
    cfg.setdefault("prefetch_metadata", True)
    cfg.setdefault("prefetch_workers", 1)

    # CSV autoload
    cfg.setdefault("csv_autoload", True)
    cfg.setdefault("csv_file", "")  # optional explicit file

    # NEW: queue limit
    cfg.setdefault("queue_limit", 30)

    return cfg


def fmt_hhmmss(seconds: float) -> str:
    if seconds < 0:
        seconds = 0
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h}:{m:02d}:{sec:02d}"


def fmt_size(num_bytes: int) -> str:
    if not num_bytes:
        return "?"
    b = float(num_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while b >= 1024 and i < len(units) - 1:
        b /= 1024.0
        i += 1
    if i == 0:
        return f"{int(b)}{units[i]}"
    return f"{b:.1f}{units[i]}"


def estimate_bytes_from_info(info: dict) -> int:
    if not isinstance(info, dict):
        return 0

    for k in ("filesize", "filesize_approx"):
        v = info.get(k)
        if isinstance(v, (int, float)) and v > 0:
            return int(v)

    rf = info.get("requested_formats")
    if isinstance(rf, list) and rf:
        total = 0
        for f in rf:
            if not isinstance(f, dict):
                continue
            v = f.get("filesize") or f.get("filesize_approx")
            if isinstance(v, (int, float)) and v > 0:
                total += int(v)
        if total > 0:
            return total

    dur = info.get("duration")
    tbr = info.get("tbr")
    if isinstance(dur, (int, float)) and dur > 0 and isinstance(tbr, (int, float)) and tbr > 0:
        return int((tbr * 1000.0 / 8.0) * float(dur))

    return 0


def find_csv(script_dir: Path, cfg: dict):
    p = (cfg.get("csv_file") or "").strip()
    if p:
        cp = Path(p)
        if not cp.is_absolute():
            cp = (script_dir / cp).resolve()
        if cp.exists() and cp.is_file():
            return cp

    for f in sorted(script_dir.glob("*.csv")):
        if f.is_file():
            return f
    return None


def load_urls_from_csv(csv_path: Path) -> list[str]:
    urls: list[str] = []
    try:
        with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                for cell in row:
                    if not cell:
                        continue
                    for u in URL_FINDER.findall(str(cell)):
                        urls.append(u.strip())
    except Exception:
        return []

    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


@dataclass
class Job:
    jid: int
    job_id: str  # filename base (timestamp + jid)
    url: str
    status: str = "pending"  # pending/queued/downloading/done/fail

    downloaded: int = 0
    total: int = 0
    speed: float = 0.0
    eta: int = 0

    added_at: float = 0.0
    enqueued_at: float = 0.0
    started_at: float = 0.0
    finished_at: float = 0.0

    error: str = ""
    final_path: str = ""

    last_eta: int = -1
    last_eta_check_at: float = 0.0
    eta_bad: bool = False

    meta_done: bool = False


class GrabberDaemon:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.out_dir = Path(cfg["download_dir"]).expanduser().resolve()
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self.q: queue.Queue[str] = queue.Queue()  # holds job_id
        self.jobs: dict[str, Job] = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        self.meta_q: queue.Queue[str] = queue.Queue()

        # pending (not yet in queue)
        self.pending: deque[str] = deque()

        # counters
        self._jid_counter = 0

        # columns
        self.W_JID = 4
        self.W_ID = 17
        self.W_TOTAL = 9

        self.queue_limit = int(cfg.get("queue_limit", 30))

        # csv info
        self.csv_loaded_from: str = ""
        self.csv_loaded_count: int = 0

    def _next_jid(self) -> int:
        with self.lock:
            self._jid_counter += 1
            return self._jid_counter

    def _new_job_id(self, jid: int) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{ts}_{jid:04d}"

    def _queued_count_locked(self) -> int:
        return sum(1 for j in self.jobs.values() if j.status == "queued")

    def _enqueue_job_into_queue_locked(self, job_id: str):
        j = self.jobs[job_id]
        if j.status != "pending":
            return
        j.status = "queued"
        j.enqueued_at = time.time()
        self.q.put(job_id)
        if self.cfg.get("prefetch_metadata", True):
            self.meta_q.put(job_id)

    def add_url(self, url: str) -> str:
        url = (url or "").strip()
        if not url:
            raise ValueError("empty")

        jid = self._next_jid()
        job_id = self._new_job_id(jid)
        job = Job(jid=jid, job_id=job_id, url=url, added_at=time.time())

        with self.lock:
            self.jobs[job_id] = job
            if self._queued_count_locked() < self.queue_limit:
                self._enqueue_job_into_queue_locked(job_id)
            else:
                self.pending.append(job_id)

        return job_id

    def add_many_urls(self, urls: list[str]) -> int:
        n = 0
        for u in urls:
            try:
                self.add_url(u)
                n += 1
            except Exception:
                continue
        return n

    def _bar(self, downloaded: int, total: int, width: int = 24) -> str:
        if total <= 0:
            return "[" + ("?" * width) + "]"
        pct = max(0.0, min(1.0, downloaded / total))
        filled = int(width * pct)
        return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"

    def _pad(self, txt: str, width: int) -> str:
        if len(txt) >= width:
            return txt[:width]
        return txt + (" " * (width - len(txt)))

    def _rpad(self, txt: str, width: int) -> str:
        if len(txt) >= width:
            return txt[-width:]
        return (" " * (width - len(txt))) + txt

    def _render(self, first: bool = False):
        try:
            if first:
                ansi_clear_screen()
            else:
                ansi_home()
                ansi_clear_to_end()

            with self.lock:
                jobs = list(self.jobs.values())
                qsize = self.q.qsize()
                fr = int(self.cfg["concurrent_fragments"])
                meta_pending = self.meta_q.qsize()
                pending_cnt = len(self.pending)
                csv_info = self.csv_loaded_from or "none"
                csv_count = self.csv_loaded_count
                qlimit = self.queue_limit
                workers = int(self.cfg["max_workers"])

            active = sum(1 for j in jobs if j.status == "downloading")
            done = sum(1 for j in jobs if j.status == "done")
            fail = sum(1 for j in jobs if j.status == "fail")
            queued = sum(1 for j in jobs if j.status == "queued")

            line = "-" * 100
            print(line)
            print("Video Grabber (paste URL + Enter). Ctrl+C to quit.")
            print(f"Output: {self.out_dir}")
            print(
                f"Workers: {workers} | Frags: {fr} | "
                f"Queue: {qsize}/{qlimit} | pending={pending_cnt} | "
                f"queued={queued} active={active} done={done} fail={fail} | metaQ={meta_pending}"
            )
            print(f"CSV: {csv_info} ({csv_count})")
            print(line)
            print()

            col_jid = self._pad("ID", self.W_JID)
            col_id = self._pad("JOB_ID", self.W_ID)
            col_total = self._rpad("TOTAL", self.W_TOTAL)
            print(f"{col_jid} {col_id} {col_total} STATUS/PROGRESS")
            print("-" * (self.W_JID + 1 + self.W_ID + 1 + self.W_TOTAL + 1 + 70))

            now = time.time()

            queued_jobs = sorted([j for j in jobs if j.status == "queued"], key=lambda j: j.enqueued_at)
            active_jobs = sorted([j for j in jobs if j.status == "downloading"], key=lambda j: j.started_at or 0.0)
            done_jobs = sorted([j for j in jobs if j.status == "done"], key=lambda j: j.finished_at or 0.0, reverse=True)
            fail_jobs = sorted([j for j in jobs if j.status == "fail"], key=lambda j: j.finished_at or 0.0, reverse=True)

            shown = 0
            MAX_LINES = 25

            RIGHT_LABEL_W = 12  # "ENQ 00:00:00" / "ELAP 0:00:00"
            QUEUED_MIDDLE_W = 64  # pad so ENQ ends near ELAP column

            def render_row(j: Job):
                jid_txt = self._pad(f"{j.jid:04d}", self.W_JID)
                jid_col = c(jid_txt, YELLOW)

                id_plain = self._pad(j.job_id, self.W_ID)
                id_col = c(id_plain, BLUE)

                total_txt = fmt_size(j.total)
                total_plain = self._rpad(total_txt, self.W_TOTAL)
                total_col = c(total_plain, MAGENTA)

                if j.status == "queued":
                    enq = time.strftime("%H:%M:%S", time.localtime(j.enqueued_at)) if j.enqueued_at else "??:??:??"
                    status = c("[QUEUED]", DIM)
                    middle = self._pad(status, QUEUED_MIDDLE_W)
                    right = self._rpad(f"ENQ {enq}", RIGHT_LABEL_W)
                    print(f"{jid_col} {id_col} {total_col} {middle}{right}")
                    return

                if j.status == "downloading":
                    bar = c(self._bar(j.downloaded, j.total), YELLOW)
                    pct = f"{(j.downloaded / j.total * 100):5.1f}%" if j.total else "  ?.?%"
                    sp = c(f"{(j.speed / 1024 / 1024):5.1f}MB/s", GREEN) if j.speed else c("  ?.?MB/s", GREEN)

                    eta_txt = fmt_hhmmss(j.eta)
                    elap_txt = fmt_hhmmss((now - j.started_at) if j.started_at else 0)

                    eta_color = RED if j.eta_bad else CYAN
                    eta_part = c("ETA ", DIM) + c(eta_txt, eta_color)
                    elap_part = c("ELAP ", DIM) + elap_txt

                    print(f"{jid_col} {id_col} {total_col} {bar} {pct} {sp} {eta_part} {elap_part}")
                    return

                if j.status == "done":
                    final_bytes = 0
                    if j.final_path:
                        try:
                            final_bytes = Path(j.final_path).stat().st_size
                        except Exception:
                            final_bytes = 0

                    done_total_txt = fmt_size(final_bytes) if final_bytes else fmt_size(j.total)
                    done_total = c(self._rpad(done_total_txt, self.W_TOTAL), MAGENTA)

                    elap_txt = fmt_hhmmss((j.finished_at - j.started_at) if (j.started_at and j.finished_at) else 0)
                    saved = j.final_path or "(path unknown)"
                    status = c("[DONE]", DIM)
                    print(f"{jid_col} {id_col} {done_total} {status} in {elap_txt} -> {c(saved, DIM)}")
                    return

                if j.status == "fail":
                    err = (j.error[:140] + "…") if len(j.error) > 140 else j.error
                    status = c("[FAIL]", RED)
                    print(f"{jid_col} {id_col} {total_col} {status} {err}")
                    return

            for block in (queued_jobs, active_jobs, done_jobs, fail_jobs):
                for j in block:
                    if shown >= MAX_LINES:
                        break
                    render_row(j)
                    shown += 1
                if shown >= MAX_LINES:
                    break

            remaining = (len(queued_jobs) + len(active_jobs) + len(done_jobs) + len(fail_jobs)) - shown
            if remaining > 0:
                print()
                print(f"... ({remaining} more not shown)")

            print()
            print(line)
            print("Paste URL here and press Enter:", end="", flush=True)

        except Exception as e:
            ansi_home()
            ansi_clear_to_end()
            print("UI render error:", repr(e))
            print("Paste URL here and press Enter:", end="", flush=True)

    def _build_ydl_opts(self, job: Job) -> dict:
        outtmpl = str(self.out_dir / job.job_id) + ".%(ext)s"
        headers = {"User-Agent": self.cfg["user_agent"], "Referer": job.url}
        ffmpeg_loc = (self.cfg.get("ffmpeg_path") or "").strip()

        retries = int(self.cfg["retries"])
        frag_cfg = self.cfg.get("fragment_retries")
        fragment_retries = int(frag_cfg) if frag_cfg is not None else retries

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
                    j.total = int(d.get("total_bytes") or d.get("total_bytes_estimate") or j.total or 0)
                    j.speed = float(d.get("speed") or 0.0)
                    j.eta = int(d.get("eta") or 0)

                    now2 = time.time()
                    if j.last_eta_check_at == 0.0:
                        j.last_eta_check_at = now2
                        j.last_eta = j.eta
                        j.eta_bad = False
                    elif now2 - j.last_eta_check_at >= 2.0 and j.last_eta >= 0 and j.eta >= 0:
                        dt = now2 - j.last_eta_check_at
                        expected_drop = max(1, int(dt))
                        actual_drop = j.last_eta - j.eta
                        j.eta_bad = actual_drop < max(1, expected_drop // 2)
                        j.last_eta_check_at = now2
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

    # feeder: move pending -> queue until queue_limit reached
    def feeder_loop(self):
        while not self.stop_event.is_set():
            moved = False
            with self.lock:
                while self.pending and self._queued_count_locked() < self.queue_limit:
                    job_id = self.pending.popleft()
                    if job_id in self.jobs and self.jobs[job_id].status == "pending":
                        self._enqueue_job_into_queue_locked(job_id)
                        moved = True
            if not moved:
                time.sleep(0.2)

    def meta_loop(self):
        while not self.stop_event.is_set():
            try:
                job_id = self.meta_q.get(timeout=0.2)
            except queue.Empty:
                continue

            try:
                with self.lock:
                    job = self.jobs.get(job_id)

                if not job or job.meta_done:
                    continue

                headers = {"User-Agent": self.cfg["user_agent"], "Referer": job.url}
                ffmpeg_loc = (self.cfg.get("ffmpeg_path") or "").strip()

                opts = {
                    "quiet": True,
                    "no_warnings": True,
                    "noprogress": True,
                    "skip_download": True,
                    "http_headers": headers,
                    "socket_timeout": int(self.cfg["timeout_sec"]),
                    "retries": int(self.cfg["retries"]),
                }
                if ffmpeg_loc:
                    opts["ffmpeg_location"] = ffmpeg_loc

                with YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(job.url, download=False)

                size_bytes = estimate_bytes_from_info(info)

                with self.lock:
                    j = self.jobs.get(job_id)
                    if j:
                        if j.total <= 0 and size_bytes > 0:
                            j.total = int(size_bytes)
                        j.meta_done = True

            except Exception:
                with self.lock:
                    j = self.jobs.get(job_id)
                    if j:
                        j.meta_done = True
            finally:
                self.meta_q.task_done()

    def worker_loop(self):
        while not self.stop_event.is_set():
            try:
                job_id = self.q.get(timeout=0.2)
            except queue.Empty:
                continue

            with self.lock:
                job = self.jobs.get(job_id)
                if not job:
                    self.q.task_done()
                    continue
                if job.started_at == 0.0:
                    job.started_at = time.time()
                job.status = "downloading"

            try:
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
                self.q.task_done()

    def monitor_loop(self):
        first = True
        refresh = float(self.cfg.get("refresh_sec", 0.6))
        while not self.stop_event.is_set():
            self._render(first=first)
            first = False
            time.sleep(refresh)

    def start(self):
        # download workers
        for _ in range(int(self.cfg["max_workers"])):
            threading.Thread(target=self.worker_loop, daemon=True).start()

        # metadata workers
        if self.cfg.get("prefetch_metadata", True):
            for _ in range(int(self.cfg.get("prefetch_workers", 1))):
                threading.Thread(target=self.meta_loop, daemon=True).start()

        # feeder
        threading.Thread(target=self.feeder_loop, daemon=True).start()

        # UI
        threading.Thread(target=self.monitor_loop, daemon=True).start()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    daemon = GrabberDaemon(cfg)
    daemon.start()

    # CSV autoload at startup
    if cfg.get("csv_autoload", True):
        script_dir = Path(__file__).resolve().parent
        csv_path = find_csv(script_dir, cfg)
        if csv_path:
            urls = load_urls_from_csv(csv_path)
            count = daemon.add_many_urls(urls)
            daemon.csv_loaded_from = csv_path.name
            daemon.csv_loaded_count = count

    try:
        while True:
            line = input().strip()
            if not line:
                continue
            candidate = line.split()[0].strip()
            try:
                daemon.add_url(candidate)
            except Exception:
                pass
    except KeyboardInterrupt:
        daemon.stop_event.set()
        print("\nExiting...")


if __name__ == "__main__":
    main()
