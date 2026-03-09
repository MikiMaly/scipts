import argparse
import os
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
    colorama_init()  # enables ANSI on Windows terminals
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


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    cfg.setdefault("download_dir", "./downloads")
    cfg.setdefault("max_workers", 10)
    cfg.setdefault("timeout_sec", 60)
    cfg.setdefault("format", "bv*+ba/b")
    cfg.setdefault("retries", 20)
    cfg.setdefault("fragment_retries", None)  # optional
    cfg.setdefault("concurrent_fragments", 3)
    cfg.setdefault("user_agent", "Mozilla/5.0")
    cfg.setdefault("ffmpeg_path", "")
    return cfg


def fmt_hhmmss(seconds: float) -> str:
    """Always H:MM:SS (0:05:12 etc.)."""
    if seconds < 0:
        seconds = 0
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h}:{m:02d}:{sec:02d}"


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

    # ETA health heuristic
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

    def _render(self):
        os.system("cls")
        with self.lock:
            jobs = list(self.jobs.values())
            qsize = self.q.qsize()

        active = sum(1 for j in jobs if j.status == "downloading")
        done = sum(1 for j in jobs if j.status == "done")
        fail = sum(1 for j in jobs if j.status == "fail")
        queued = sum(1 for j in jobs if j.status == "queued")

        print("Video Grabber (paste URL + Enter). Ctrl+C to quit.\n")
        print(f"Output: {self.out_dir}")
        print(
            f"Workers: {self.cfg['max_workers']} | Frags: {self.cfg['concurrent_fragments']} | "
            f"Queue: {qsize} | queued={queued} active={active} done={done} fail={fail}\n"
        )

        jobs_sorted = sorted(jobs, key=lambda j: (j.status != "downloading", j.status != "queued", j.job_id))
        now = time.time()

        for j in jobs_sorted[:25]:
            jid = c(j.job_id, BLUE)

            if j.status == "downloading":
                bar = c(self._bar(j.downloaded, j.total), YELLOW)
                pct = f"{(j.downloaded / j.total * 100):5.1f}%" if j.total else "  ?.?%"
                sp = c(f"{(j.speed / 1024 / 1024):5.1f}MB/s", GREEN) if j.speed else c("  ?.?MB/s", GREEN)

                eta_txt = fmt_hhmmss(j.eta)
                elap_txt = fmt_hhmmss((now - j.started_at) if j.started_at else 0)

                # ETA coloring: red if "stalling", otherwise cyan
                eta_color = RED if j.eta_bad else CYAN
                eta_part = c("ETA ", DIM) + c(eta_txt, eta_color)

                # ELAP value white (no color wrapper), label dim
                elap_part = c("ELAP ", DIM) + elap_txt

                print(f"{jid} {bar} {pct} {sp} {eta_part} {elap_part}")

            elif j.status == "queued":
                print(f"{jid} {c('[QUEUED]', DIM)} {j.url}")

            elif j.status == "done":
                elap_txt = fmt_hhmmss((j.finished_at - j.started_at) if (j.started_at and j.finished_at) else 0)
                saved = j.final_path or "(path unknown)"
                print(f"{jid} {c('[DONE]', DIM)} in {elap_txt} -> {c(saved, DIM)}")

            elif j.status == "fail":
                err = (j.error[:140] + "…") if len(j.error) > 140 else j.error
                print(f"{jid} {c('[FAIL]', RED)} {err}")

        if len(jobs_sorted) > 25:
            print(f"\n... ({len(jobs_sorted) - 25} more not shown)")

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

                    # ETA stalling heuristic (as-is):
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
        while not self.stop_event.is_set():
            self._render()
            time.sleep(0.5)

    def start(self):
        for _ in range(int(self.cfg["max_workers"])):
            t = threading.Thread(target=self.worker_loop, daemon=True)
            t.start()

        tmon = threading.Thread(target=self.monitor_loop, daemon=True)
        tmon.start()


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
                job_id = daemon.enqueue_url(candidate)
                print(f"Enqueued {job_id}")
            except Exception:
                print("Paste a URL and press Enter.")
    except KeyboardInterrupt:
        daemon.stop_event.is_set()
        print("\nExiting...")


if __name__ == "__main__":
    main()
