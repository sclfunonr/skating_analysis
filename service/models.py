"""
Shared data models used by the pipeline service, API, and CLI.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any
import time
import uuid


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class JobStatus(str, Enum):
    PENDING    = "pending"
    RUNNING    = "running"
    DONE       = "done"
    FAILED     = "failed"


class StepName(str, Enum):
    UPLOAD     = "upload"
    ANALYZE    = "analyze"
    EXTRACT    = "extract"
    TRANSLATE  = "translate"
    GENERATE   = "generate"
    POLL       = "poll"
    DONE       = "done"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@dataclass
class PipelineConfig:
    spark_ip:   str = "192.168.1.125"
    spark_user: str = "suetchun"

    # Spark host paths (used for scp)
    @property
    def spark_videos_host(self) -> str:
        return f"/home/{self.spark_user}/ball/videos"

    @property
    def spark_inputs_host(self) -> str:
        return f"/home/{self.spark_user}/ball/inputs"

    @property
    def spark_outputs_host(self) -> str:
        return f"/home/{self.spark_user}/ball/cosmos-predict2.5/outputs/latest"

    # Spark container paths (used for file:// URLs and input_path)
    spark_videos_container:  str = "/videos"
    spark_inputs_container:  str = "/workspace/inputs"

    # Local Legion 7 paths
    local_base: Path = field(default_factory=lambda: Path.home() / "skating_project")

    @property
    def outputs_dir(self) -> Path:
        return self.local_base / "outputs"

    @property
    def analysis_dir(self) -> Path:
        return self.local_base / "analysis"


# ---------------------------------------------------------------------------
# Job tracking
# ---------------------------------------------------------------------------
@dataclass
class JobUpdate:
    timestamp: float = field(default_factory=time.time)
    message:   str   = ""
    progress:  int   = 0     # 0-100
    step:      StepName = StepName.UPLOAD


@dataclass
class Job:
    id:             str       = field(default_factory=lambda: str(uuid.uuid4())[:8])
    status:         JobStatus = JobStatus.PENDING
    video_name:     str       = ""
    reference_name: str | None = None
    created_at:     float     = field(default_factory=time.time)
    updates:        list[JobUpdate] = field(default_factory=list)
    error:          str | None = None
    result:         "AnalysisResult | None" = None

    def log(self, message: str, progress: int = 0, step: StepName = StepName.UPLOAD) -> None:
        self.updates.append(JobUpdate(message=message, progress=progress, step=step))

    def latest_log(self) -> str:
        return self.updates[-1].message if self.updates else ""

    def progress(self) -> int:
        return self.updates[-1].progress if self.updates else 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "id":             self.id,
            "status":         self.status.value,
            "video_name":     self.video_name,
            "reference_name": self.reference_name,
            "created_at":     self.created_at,
            "progress":       self.progress(),
            "latest_log":     self.latest_log(),
            "error":          self.error,
            "has_result":     self.result is not None,
        }


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------
@dataclass
class AnalysisResult:
    output_name:   str
    analysis:      str
    prompt:        str
    analysis_file: Path
    prompt_file:   Path
    output_video:  Path | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "output_name":   self.output_name,
            "analysis":      self.analysis,
            "prompt":        self.prompt,
            "output_video":  str(self.output_video) if self.output_video else None,
            "analysis_file": str(self.analysis_file),
            "prompt_file":   str(self.prompt_file),
        }
