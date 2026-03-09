import argparse
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

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
MAGENTA = "35"
DIM = "2"

# ANSI helpers
CSI = "\x1b["
def ansi_clear_screen():
    # Clear screen + move cursor to home
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
    cfg.setdefault("refresh_sec", 10)
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


@dataclass
class Job:
    job_id: str
    url: str
    status: str = "queued"  # queued/downloading/done/fail

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


class GrabberDaemon:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.out_dir = Path(cfg["download_dir"]).expanduser().resolve()
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self.q: queue.Queue[Job] = queue.Queue()
        self.jobs: dict[str, Job] = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        # column widths (tuneable)
        self.W_ID = 19
        self.W_TOTAL = 10

    def _new_job_id(self) -> str:
        base = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_id = base
        i = 2
        with self.lock:
            while job_id in self.jobs:
                job_id = f"{base}_{i}"
                i += 1
        return job_id

    def enqueue_url(self, url: str) -> str:
        url = url.strip()
        if not url:
            raise ValueError("empty")
        job_id = self._new_job_id()
        job = Job(job_id=job_id, url=url)
        with self.lock:
            self.jobs[job_id] = job
        self.q.put(job)
        return job_id

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
        # no cls: just move cursor home and redraw
        if first:
            ansi_clear_screen()
        else:
            ansi_home()
            ansi_clear_to_end()

        with self.lock:
            jobs = list(self.jobs.values())
            qsize = self.q.qsize()
            fr = int(self.cfg["concurrent_fragments"])

        active = sum(1 for j in jobs if j.status == "downloading")
        done = sum(1 for j in jobs if j.status == "done")
        fail = sum(1 for j in jobs if j.status == "fail")
        queued = sum(1 for j in jobs if j.status == "queued")

        header1 = "Video Grabber (paste URL + Enter). Ctrl+C to quit."
        header2 = f"Output: {self.out_dir}"
        header3 = f"Workers: {self.cfg['max_workers']} | Frags: {fr} | Queue: {qsize} | queued={queued} active={active} done={done} fail={fail}"

        print(header1)
        print(header2)
        print(header3)
        print()

        # table header
        col_id = self._pad("JOB_ID", self.W_ID)
        col_total = self._rpad("TOTAL", self.W_TOTAL)
        print(f"{col_id} {col_total}  STATUS/PROGRESS")
        print("-" * (self.W_ID + 1 + self.W_TOTAL + 2 + 60))

        jobs_sorted = sorted(jobs, key=lambda j: (j.status != "downloading", j.status != "queued", j.job_id))
        now = time.time()

        for j in jobs_sorted[:25]:
            jid_plain = self._pad(j.job_id, self.W_ID)
            jid = c(jid_plain, BLUE)

            total_txt = fmt_size(j.total)
            total_plain = self._rpad(total_txt, self.W_TOTAL)
            total_col = c(total_plain, MAGENTA)

            if j.status == "downloading":
                bar_plain = self._bar(j.downloaded, j.total)
                bar = c(bar_plain, YELLOW)
                pct = f"{(j.downloaded / j.total * 100):5.1f}%" if j.total else "  ?.?%"
                sp = c(f"{(j.speed / 1024 / 1024):5.1f}MB/s", GREEN) if j.speed else c("  ?.?MB/s", GREEN)

                eta_txt = fmt_hhmmss(j.eta)
                elap_txt = fmt_hhmmss((now - j.started_at) if j.started_at else 0)

                eta_color = RED if j.eta_bad else CYAN
                eta_part = c("ETA ", DIM) + c(eta_txt, eta_color)
                elap_part = c("ELAP ", DIM) + elap_txt

                print(f"{jid} {total_col}  {bar} {pct} {sp} {eta_part} {elap_part}")

            elif j.status == "queued":
                status = c("[QUEUED]", DIM)
                print(f"{jid} {total_col}  {status} {j.url}")

            elif j.status == "done":
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
                print(f"{jid} {done_total}  {status} in {elap_txt} -> {c(saved, DIM)}")

            elif j.status == "fail":
                err = (j.error[:140] + "…") if len(j.error) > 140 else j.error
                status = c("[FAIL]", RED)
                print(f"{jid} {total_col}  {status} {err}")

        if len(jobs_sorted) > 25:
            print()
            print(f"... ({len(jobs_sorted) - 25} more not shown)")

        print()
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
                    j.total = int(d.get("total_bytes") or d.get("total_bytes_estimate") or 0)
                    j.speed = float(d.get("speed") or 0.0)
                    j.eta = int(d.get("eta") or 0)

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

    def worker_loop(self):
        while not self.stop_event.is_set():
            try:
                job = self.q.get(timeout=0.2)
            except queue.Empty:
                continue

            with self.lock:
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
        refresh = float(self.cfg.get("refresh_sec", 0.5))
        while not self.stop_event.is_set():
            self._render(first=first)
            first = False
            time.sleep(refresh)

    def start(self):
        for _ in range(int(self.cfg["max_workers"])):
            threading.Thread(target=self.worker_loop, daemon=True).start()
        threading.Thread(target=self.monitor_loop, daemon=True).start()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    daemon = GrabberDaemon(cfg)
    daemon.start()

    try:
        while True:
            line = input().strip()
            if not line:
                continue
            candidate = line.split()[0].strip()
            try:
                daemon.enqueue_url(candidate)
            except Exception:
                pass
    except KeyboardInterrupt:
        daemon.stop_event.set()
        print("\nExiting...")


if __name__ == "__main__":
    main()
