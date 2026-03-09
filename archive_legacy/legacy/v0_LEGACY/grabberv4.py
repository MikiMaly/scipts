import argparse
import os
import queue
import re
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml
from yt_dlp import YoutubeDL

# --- ANSI + colors (Windows-friendly) ---
ANSI = False
try:
    from colorama import init as colorama_init
    colorama_init()
    ANSI = True
except Exception:
    ANSI = False


def esc(s: str) -> str:
    # ANSI escape sequence helper
    return f"\x1b[{s}" if ANSI else ""


def color(text: str, code: str) -> str:
    return f"\x1b[{code}m{text}\x1b[0m" if ANSI else text


BLUE = "34"
YELLOW = "33"
GREEN = "32"
RED = "31"
DIM = "2"


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    cfg.setdefault("download_dir", "./downloads")
    cfg.setdefault("max_workers", 10)
    cfg.setdefault("timeout_sec", 60)
    cfg.setdefault("format", "bv*+ba/b")
    cfg.setdefault("retries", 20)
    cfg.setdefault("concurrent_fragments", 3)
    cfg.setdefault("user_agent", "Mozilla/5.0")
    cfg.setdefault("ffmpeg_path", "")
    return cfg


def fmt_hhmmss(seconds: float) -> str:
    if seconds < 0:
        seconds = 0
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h}:{m:02d}:{sec:02d}"


def safe_first_url_token(line: str) -> str:
    return line.strip().split()[0].strip()


def progress_bar(downloaded: int, total: int, width: int = 22) -> str:
    if total <= 0:
        return "[" + ("?" * width) + "]"
    pct = max(0.0, min(1.0, downloaded / total))
    filled = int(width * pct)
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


@dataclass
class Job:
    idx: int
    job_id: str
    url: str

    status: str = "queued"  # queued/downloading/done/fail
    downloaded: int = 0
    total: int = 0
    speed: float = 0.0
    eta: int = 0

    started_at: float = 0.0
    finished_at: float = 0.0

    final_path: str = ""
    error: str = ""

    # ETA stalling heuristic
    last_eta: int = -1
    last_eta_check_at: float = 0.0
    eta_bad: bool = False


# ---------------- UI: Fixed-lines (ANSI) ----------------
class FixedLineUI:
    """
    Keeps one fixed line per job.
    Uses: save cursor, move to row, rewrite line, restore cursor.
    Works best in Windows Terminal / modern PowerShell.
    """
    def __init__(self, output_dir: Path, workers: int):
        self.lock = threading.Lock()
        self.output_dir = output_dir
        self.workers = workers
        self.job_lines = 0

        # print static header once
        self._print_header()

    def _print_header(self):
        print("Video Grabber (paste URL + Enter). Ctrl+C to quit.")
        print("Legend: bar=yellow, id=blue, speed=green, ETA=red if stalling")
        print(f"Output: {self.output_dir}")
        print(f"Workers: {self.workers}")
        print("\nJobs:")

    def ensure_job_lines(self, count: int):
        with self.lock:
            if count <= self.job_lines:
                return
            for _ in range(count - self.job_lines):
                print("")  # allocate a line
            self.job_lines = count
            sys.stdout.flush()

    def _job_row_number(self, job_idx: int) -> int:
        # Rows are 1-indexed in ANSI CUP command.
        # Header printed 5 lines + 1 blank + "Jobs:" line = total 6 lines before job lines start.
        # Let's compute it robustly: we printed:
        # 1) title
        # 2) legend
        # 3) output
        # 4) workers
        # 5) blank line
        # 6) "Jobs:"
        # Then job line 1 is row 7.
        return 6 + job_idx

    def render_job(self, job: Job):
        with self.lock:
            # Save cursor position
            if ANSI:
                sys.stdout.write(esc("s"))  # save cursor

                # Move cursor to job line (row, col 1)
                row = self._job_row_number(job.idx)
                sys.stdout.write(esc(f"{row};1H"))

            # Build line
            jid = color(job.job_id, BLUE)
            if job.status == "queued":
                line = f"{jid} {color('[QUEUED]', DIM)} {job.url}"
            elif job.status == "downloading":
                b = color(progress_bar(job.downloaded, job.total), YELLOW)
                pct = f"{(job.downloaded / job.total * 100):5.1f}%" if job.total else "  ?.?%"
                sp = color(f"{(job.speed / 1024 / 1024):5.1f}MB/s" if job.speed else "  ?.?MB/s", GREEN)
                eta_txt = fmt_hhmmss(job.eta)
                elap_txt = fmt_hhmmss((time.time() - job.started_at) if job.started_at else 0)
                eta_col = RED if job.eta_bad else DIM
                line = f"{jid} {b} {pct} {sp} {color('ETA ' + eta_txt, eta_col)} {color('ELAP ' + elap_txt, DIM)}"
            elif job.status == "done":
                elap_txt = fmt_hhmmss((job.finished_at - job.started_at) if (job.started_at and job.finished_at) else 0)
                saved = job.final_path or "(path unknown)"
                line = f"{jid} {color('[DONE]', DIM)} in {color(elap_txt, DIM)} -> {color(saved, DIM)}"
            else:
                err = (job.error[:140] + "…") if len(job.error) > 140 else job.error
                line = f"{jid} {color('[FAIL]', RED)} {err}"

            # Write line and clear rest of that terminal line
            sys.stdout.write(line[:320])
            if ANSI:
                sys.stdout.write(esc("K"))  # clear to end of line

                # Restore cursor position
                sys.stdout.write(esc("u"))

            sys.stdout.flush()


# ---------------- UI: Fallback (safe redraw) ----------------
class RedrawUI:
    def __init__(self, output_dir: Path, workers: int):
        self.output_dir = output_dir
        self.workers = workers
        self.lock = threading.Lock()
        self.last_render = 0.0

    def ensure_job_lines(self, count: int):
        # no-op
        return

    def render_all(self, jobs: list[Job]):
        with self.lock:
            os.system("cls")
            active = sum(1 for j in jobs if j.status == "downloading")
            done = sum(1 for j in jobs if j.status == "done")
            fail = sum(1 for j in jobs if j.status == "fail")
            queued = sum(1 for j in jobs if j.status == "queued")

            print("Video Grabber (paste URL + Enter). Ctrl+C to quit.\n")
            print(f"Output: {self.output_dir}")
            print(f"Workers: {self.workers} | queued={queued} active={active} done={done} fail={fail}\n")

            for j in jobs[-25:]:
                if j.status == "downloading":
                    b = progress_bar(j.downloaded, j.total)
                    pct = f"{(j.downloaded / j.total * 100):5.1f}%" if j.total else "  ?.?%"
                    sp = f"{(j.speed / 1024 / 1024):5.1f}MB/s" if j.speed else "  ?.?MB/s"
                    eta_txt = fmt_hhmmss(j.eta)
                    elap_txt = fmt_hhmmss((time.time() - j.started_at) if j.started_at else 0)
                    print(f"{j.job_id} {b} {pct} {sp} ETA {eta_txt} ELAP {elap_txt}")
                elif j.status == "queued":
                    print(f"{j.job_id} [QUEUED] {j.url}")
                elif j.status == "done":
                    elap_txt = fmt_hhmmss((j.finished_at - j.started_at) if (j.started_at and j.finished_at) else 0)
                    print(f"{j.job_id} [DONE] in {elap_txt} -> {j.final_path}")
                else:
                    print(f"{j.job_id} [FAIL] {j.error}")

            print("\nPaste URL here and press Enter:")

    def render_job(self, job: Job):
        # in redraw mode we refresh periodically from a monitor thread
        return


# ---------------- Daemon ----------------
class GrabberDaemon:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.out_dir = Path(cfg["download_dir"]).expanduser().resolve()
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self.q: queue.Queue[Job] = queue.Queue()
        self.jobs: list[Job] = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self._job_counter = 0

        # choose UI mode
        # If ANSI is available, use fixed line UI; otherwise safe redraw.
        self.ui = FixedLineUI(self.out_dir, int(cfg["max_workers"])) if ANSI else RedrawUI(self.out_dir, int(cfg["max_workers"]))

        # redraw monitor if needed
        self.monitor_thread = None
        if isinstance(self.ui, RedrawUI):
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()

    def _monitor_loop(self):
        while not self.stop_event.is_set():
            with self.lock:
                jobs_snapshot = list(self.jobs)
            self.ui.render_all(jobs_snapshot)
            time.sleep(0.5)

    def _new_job_id(self) -> str:
        base = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_id = base
        i = 2
        with self.lock:
            existing = {j.job_id for j in self.jobs}
            while job_id in existing:
                job_id = f"{base}_{i}"
                i += 1
        return job_id

    def enqueue_url(self, url: str) -> str:
        with self.lock:
            self._job_counter += 1
            idx = self._job_counter
            job_id = self._new_job_id()
            job = Job(idx=idx, job_id=job_id, url=url)
            self.jobs.append(job)

        self.ui.ensure_job_lines(len(self.jobs))
        self.ui.render_job(job)

        self.q.put(job)
        return job_id

    def _build_ydl_opts(self, job: Job) -> dict:
        outtmpl = str(self.out_dir / job.job_id) + ".%(ext)s"

        headers = {"User-Agent": self.cfg["user_agent"], "Referer": job.url}
        ffmpeg_loc = (self.cfg.get("ffmpeg_path") or "").strip()

        def hook(d):
            with self.lock:
                st = d.get("status")
                if st == "downloading":
                    if job.started_at == 0.0:
                        job.started_at = time.time()
                    job.status = "downloading"
                    job.downloaded = int(d.get("downloaded_bytes") or 0)
                    job.total = int(d.get("total_bytes") or d.get("total_bytes_estimate") or 0)
                    job.speed = float(d.get("speed") or 0.0)
                    job.eta = int(d.get("eta") or 0)

                    # ETA stalling heuristic (every ~2s)
                    now = time.time()
                    if job.last_eta_check_at == 0.0:
                        job.last_eta_check_at = now
                        job.last_eta = job.eta
                        job.eta_bad = False
                    elif now - job.last_eta_check_at >= 2.0 and job.last_eta >= 0 and job.eta >= 0:
                        dt = now - job.last_eta_check_at
                        expected_drop = max(1, int(dt))
                        actual_drop = job.last_eta - job.eta
                        job.eta_bad = actual_drop < max(1, expected_drop // 2)
                        job.last_eta_check_at = now
                        job.last_eta = job.eta

                elif st == "finished":
                    fn = d.get("filename")
                    if fn:
                        job.final_path = str(Path(fn).resolve())
                elif st == "error":
                    job.status = "fail"

            # update UI for this job
            if isinstance(self.ui, FixedLineUI):
                self.ui.render_job(job)

        opts = {
            "format": self.cfg["format"],

            # merge A+V into one file; prefer MP4 container
            "merge_output_format": "mp4",
            "remuxvideo": "mp4",

            "outtmpl": outtmpl,
            "retries": int(self.cfg["retries"]),
            "fragment_retries": int(self.cfg["retries"]),
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
                job.status = "downloading"
                if job.started_at == 0.0:
                    job.started_at = time.time()
            if isinstance(self.ui, FixedLineUI):
                self.ui.render_job(job)

            try:
                opts = self._build_ydl_opts(job)
                with YoutubeDL(opts) as ydl:
                    ydl.extract_info(job.url, download=True)

                mp4 = self.out_dir / f"{job.job_id}.mp4"
                if mp4.exists():
                    job.final_path = str(mp4.resolve())
                elif not job.final_path:
                    matches = sorted(self.out_dir.glob(job.job_id + ".*"),
                                     key=lambda p: p.stat().st_mtime,
                                     reverse=True)
                    if matches:
                        job.final_path = str(matches[0].resolve())

                with self.lock:
                    job.status = "done"
                    job.finished_at = time.time()

            except Exception as e:
                with self.lock:
                    job.status = "fail"
                    job.finished_at = time.time()
                    job.error = str(e)

            finally:
                if isinstance(self.ui, FixedLineUI):
                    self.ui.render_job(job)
                self.q.task_done()

    def start(self):
        for _ in range(int(self.cfg["max_workers"])):
            t = threading.Thread(target=self.worker_loop, daemon=True)
            t.start()


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
            token = safe_first_url_token(line)
            if not re.match(r"^https?://", token, re.IGNORECASE):
                print("Paste a valid http(s) URL.")
                continue
            daemon.enqueue_url(token)
    except KeyboardInterrupt:
        daemon.stop_event.set()
        print("\nExiting...")


if __name__ == "__main__":
    main()
