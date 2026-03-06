"""
FastAPI service — exposes the pipeline as a REST API with:
  POST /jobs              submit a new job (multipart: video + optional reference)
  GET  /jobs              list all jobs
  GET  /jobs/{id}         job status + result
  GET  /jobs/{id}/stream  SSE stream of live log lines + progress
  GET  /jobs/{id}/video   download the generated video
  GET  /health            Spark connectivity check
  POST /config            update Spark IP / user at runtime

Run with:
  uvicorn service.api:app --host 0.0.0.0 --port 7860 --reload
"""

import asyncio
import os
import threading
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse

from .models import Job, JobStatus, PipelineConfig, StepName
from .pipeline import check_spark, run_pipeline

_WEBAPP_DIR = Path(__file__).resolve().parent.parent / "webapp"

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Skating Jump Correction API",
    description="AI pipeline: Cosmos-Reason2 analysis → Cosmos-Predict2.5 video generation",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
_config = PipelineConfig()
_jobs: dict[str, Job] = {}
_upload_dir = Path.home() / "skating_project" / "uploads"
_upload_dir.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _save_upload(upload: UploadFile) -> Path:
    dest = _upload_dir / upload.filename
    dest.write_bytes(upload.file.read())
    return dest


def _run_job_thread(job: Job, video_path: Path, reference_path: Path | None, generate_video: bool = True) -> None:
    """Runs the full pipeline in a background thread."""
    job.status = JobStatus.RUNNING

    def log(msg: str) -> None:
        step = StepName.UPLOAD
        if "Reason2" in msg or "analyz" in msg.lower():
            step = StepName.ANALYZE
        elif "prompt" in msg.lower() or "placeholder" in msg.lower():
            step = StepName.EXTRACT
        elif "abstract" in msg.lower() or "visual" in msg.lower():
            step = StepName.TRANSLATE
        elif "Predict" in msg or "Generating" in msg:
            step = StepName.GENERATE
        elif "Waiting" in msg or "Still" in msg or "ready" in msg.lower():
            step = StepName.POLL
        job.log(msg, progress=job.progress(), step=step)

    def progress(pct: int) -> None:
        step = job.updates[-1].step if job.updates else StepName.UPLOAD
        job.log(job.latest_log(), progress=pct, step=step)

    try:
        result = run_pipeline(_config, video_path, reference_path, log, progress, generate_video_flag=generate_video)
        job.result = result
        job.status = JobStatus.DONE
        job.log("Job complete.", progress=100, step=StepName.DONE)
    except Exception as exc:
        job.error  = str(exc)
        job.status = JobStatus.FAILED
        job.log(f"Error: {exc}", progress=0, step=StepName.DONE)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    """Check Spark connectivity and model availability."""
    spark = check_spark(_config)
    return {
        "service": "ok",
        "spark": spark,
        "config": {
            "spark_ip":   _config.spark_ip,
            "spark_user": _config.spark_user,
        },
    }


@app.post("/config")
def update_config(spark_ip: str = Form(...), spark_user: str = Form(...)):
    """Update Spark IP and username at runtime."""
    _config.spark_ip   = spark_ip
    _config.spark_user = spark_user
    return {"spark_ip": _config.spark_ip, "spark_user": _config.spark_user}


@app.post("/jobs", status_code=201)
def submit_job(
    video: UploadFile = File(...),
    reference: UploadFile | None = File(None),
    generate_video: bool = Form(True),
):
    """Submit a new job. Returns job ID immediately; processing runs in background."""
    video_path = _save_upload(video)
    ref_path   = _save_upload(reference) if reference and reference.filename else None

    job                = Job()
    job.video_name     = video.filename
    job.reference_name = reference.filename if ref_path else None
    _jobs[job.id]      = job

    thread = threading.Thread(
        target=_run_job_thread,
        args=(job, video_path, ref_path, generate_video),
        daemon=True,
    )
    thread.start()

    return {"job_id": job.id, **job.to_dict()}


@app.get("/jobs")
def list_jobs():
    """List all jobs, most recent first."""
    return sorted(
        [j.to_dict() for j in _jobs.values()],
        key=lambda j: j["created_at"],
        reverse=True,
    )


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    """Get full job status and result."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    data = job.to_dict()
    if job.result:
        data["result"] = job.result.to_dict()
    data["logs"] = [
        {"timestamp": u.timestamp, "message": u.message, "progress": u.progress, "step": u.step}
        for u in job.updates
    ]
    return data


@app.get("/jobs/{job_id}/stream")
async def stream_job(job_id: str):
    """Server-Sent Events stream of live log lines and progress."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    async def event_generator() -> AsyncGenerator[str, None]:
        sent = 0
        while True:
            updates = job.updates
            while sent < len(updates):
                u = updates[sent]
                import json as _json
                data = _json.dumps({
                    "message":  u.message,
                    "progress": u.progress,
                    "step":     u.step,
                    "status":   job.status.value,
                })
                yield f"data: {data}\n\n"
                sent += 1

            if job.status in (JobStatus.DONE, JobStatus.FAILED):
                import json as _json
                final = _json.dumps({
                    "message":  "done" if job.status == JobStatus.DONE else f"failed: {job.error}",
                    "progress": 100 if job.status == JobStatus.DONE else 0,
                    "step":     "done",
                    "status":   job.status.value,
                })
                yield f"data: {final}\n\n"
                break

            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/jobs/{job_id}/video")
def download_video(job_id: str):
    """Download the generated corrected video."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.DONE or not job.result:
        raise HTTPException(status_code=404, detail="Video not ready")
    path = job.result.output_video
    if path is None:
        raise HTTPException(status_code=404, detail="Video was not generated (analysis-only job)")
    if not path.exists():
        raise HTTPException(status_code=404, detail="Video file missing")
    return FileResponse(
        path,
        media_type="video/mp4",
        filename=path.name,
    )


@app.get("/", response_class=HTMLResponse)
def webapp():
    """Serve the single-page webapp."""
    return (_WEBAPP_DIR / "index.html").read_text()


def main() -> None:
    """Console entrypoint for `skating-api`."""
    import uvicorn

    host = os.getenv("SKATING_API_HOST", "0.0.0.0")
    port = int(os.getenv("SKATING_API_PORT", "7860"))
    uvicorn.run("service.api:app", host=host, port=port)


if __name__ == "__main__":
    main()
