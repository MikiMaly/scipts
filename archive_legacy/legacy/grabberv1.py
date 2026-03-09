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


@dataclass
class Job:
    job_id: str
    url: str
    status: str = "queued"  # queued/downloading/done/fail
    downloaded: int = 0
    total: int = 0
    speed: float = 0.0
    eta: int = 0
    error: str = ""
    final_path: str = ""


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
        print(f"Workers: {self.cfg['max_workers']} | Queue: {qsize} | queued={queued} active={active} done={done} fail={fail}\n")

        # show downloading first
        jobs_sorted = sorted(jobs, key=lambda j: (j.status != "downloading", j.status != "queued", j.job_id))

        for j in jobs_sorted[:25]:
            if j.status == "downloading":
                bar = self._bar(j.downloaded, j.total)
                pct = f"{(j.downloaded / j.total * 100):5.1f}%" if j.total else "  ?.?%"
                sp = f"{(j.speed / 1024 / 1024):5.1f}MB/s" if j.speed else "  ?.?MB/s"
                print(f"{j.job_id} {bar} {pct} {sp} ETA {j.eta:>4}s")
            elif j.status == "queued":
                print(f"{j.job_id} [QUEUED] {j.url}")
            elif j.status == "done":
                name = Path(j.final_path).name if j.final_path else "(done)"
                print(f"{j.job_id} [DONE]  {name}")
            elif j.status == "fail":
                err = (j.error[:120] + "…") if len(j.error) > 120 else j.error
                print(f"{j.job_id} [FAIL]  {err}")

        if len(jobs_sorted) > 25:
            print(f"\n... ({len(jobs_sorted) - 25} more not shown)")

        print("\nPaste URL here and press Enter:")

    def _build_ydl_opts(self, job: Job) -> dict:
        # output filename = timestamp (job_id), ext decided by yt-dlp
        outtmpl = str(self.out_dir / job.job_id) + ".%(ext)s"

        headers = {"User-Agent": self.cfg["user_agent"], "Referer": job.url}
        ffmpeg_loc = (self.cfg.get("ffmpeg_path") or "").strip()

        def hook(d):
            with self.lock:
                j = self.jobs.get(job.job_id)
                if not j:
                    return
                st = d.get("status")
                if st == "downloading":
                    j.status = "downloading"
                    j.downloaded = int(d.get("downloaded_bytes") or 0)
                    j.total = int(d.get("total_bytes") or d.get("total_bytes_estimate") or 0)
                    j.speed = float(d.get("speed") or 0.0)
                    j.eta = int(d.get("eta") or 0)
                elif st == "finished":
                    j.status = "downloading"
                elif st == "error":
                    j.status = "fail"

        opts = {
            "format": self.cfg["format"],
            "outtmpl": outtmpl,
            "retries": int(self.cfg["retries"]),
            "fragment_retries": int(self.cfg["retries"]),
            "concurrent_fragment_downloads": int(self.cfg["concurrent_fragments"]),
            "socket_timeout": int(self.cfg["timeout_sec"]),
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "http_headers": headers,
            "merge_output_format": "mkv",
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

            try:
                opts = self._build_ydl_opts(job)
                with YoutubeDL(opts) as ydl:
                    ydl.extract_info(job.url, download=True)

                # resolve final path (best effort)
                matches = sorted(self.out_dir.glob(job.job_id + ".*"), key=lambda p: p.stat().st_mtime, reverse=True)
                with self.lock:
                    job.status = "done"
                    if matches:
                        job.final_path = str(matches[0])

            except Exception as e:
                with self.lock:
                    job.status = "fail"
                    job.error = str(e)

            finally:
                self.q.task_done()

    def monitor_loop(self):
        while not self.stop_event.is_set():
            self._render()
            time.sleep(0.5)

    def start(self):
        # workers
        for _ in range(int(self.cfg["max_workers"])):
            t = threading.Thread(target=self.worker_loop, daemon=True)
            t.start()

        # monitor
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

            # když někdo omylem vloží víc řádků (nebo text s mezerama),
            # vezmeme z toho první token, který vypadá jako URL
            candidate = line.split()[0].strip()

            try:
                job_id = daemon.enqueue_url(candidate)
                print(f"Enqueued {job_id}")
            except Exception:
                print("Not a valid input. Paste a URL and press Enter.")
    except KeyboardInterrupt:
        daemon.stop_event.set()
        print("\nExiting...")


if __name__ == "__main__":
    main()
