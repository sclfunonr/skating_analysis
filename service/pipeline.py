"""
Core pipeline logic — refactored from skating_pipeline.py.
All Spark communication, Reason2 analysis, and Predict2.5 generation lives here.
The service layer (api.py) and CLI (cli/main.py) both call into this module.
"""

import json
import requests
import subprocess
import time
from pathlib import Path
from typing import Callable

from .models import (
    JobStatus, PipelineConfig, AnalysisResult, JobUpdate
)

# ---------------------------------------------------------------------------
# Abstract coaching words Reason2 defaults to — caught and replaced in Step 3.5
# ---------------------------------------------------------------------------
ABSTRACT_WORDS = [
    "powerful", "correct", "better", "perfect", "flawless",
    "improved", "optimal", "proper", "enhanced", "timely",
    "tight tuck", "centered axis", "clean rotation", "successfully",
    "effectively", "efficiently", "appropriately", "seamlessly",
    "smooth exit", "strong takeoff", "good form", "well-executed",
]

# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------
SYSTEM_PROMPT_SINGLE = """You are an expert figure skating coach and AI video director.
You will analyze a failed figure skating jump and write a video generation prompt
that describes the VISUALLY CORRECT version for an AI video model to render.

CRITICAL: The AI video model understands VISUAL descriptions ONLY.
It cannot interpret abstract coaching words like "better technique", "more power",
"correct form", or "improve". You must translate every correction into what
a camera would literally see — exact body positions, shapes, and movements.

Respond in exactly this structure:

PART 1 - JUMP IDENTIFICATION:
- Jump type (Axel, Lutz, Flip, Loop, Salchow, Toe Loop)
- Number of intended rotations (single, double, triple, quad)
- Takeoff foot and edge
- What went wrong — described as what the camera sees, not coaching language

PART 2 - VISUAL ERRORS OBSERVED:
Describe exactly what the incorrect movement looks like on camera:
- Arms: where are they exactly?
- Legs: where are they?
- Body axis: tilt angle and direction
- Rotation: how many completed before ice contact
- Landing: exactly what happened

PART 3 - VISUAL CORRECTIONS (what the corrected version looks like on camera):
- Approach, Takeoff, Air position, Rotation count, Landing

Then end with this block using ONLY visual language:

VIDEO_PROMPT_START
[4-6 sentences of exact visual descriptions per phase.
Include COSTUME and RINK SETTING from the original video.
NEVER use: powerful, correct, better, perfect, flawless, improved,
optimal, proper, enhanced, timely, seamlessly, effectively, successfully.]
VIDEO_PROMPT_END"""

USER_PROMPT_SINGLE = """Watch this figure skating video carefully.
Describe EXACTLY what you see visually — the precise positions of the skater's
arms, legs, and body during each phase of the jump.
Then describe what those same body parts should look like in the correctly
executed version, using only visual terms.
End with VIDEO_PROMPT_START and VIDEO_PROMPT_END tags."""

SYSTEM_PROMPT_WITH_REFERENCE = """You are an expert figure skating coach and AI video director.
You will be shown TWO videos:
  VIDEO 1: A figure skater attempting a jump that fails
  VIDEO 2: A reference video of the SAME type of jump executed successfully

Your job:
1. Compare the two videos visually
2. Write a video generation prompt that describes VIDEO 2's correct motion
   but with VIDEO 1's costume and rink setting transplanted onto it

CRITICAL: The AI video model understands VISUAL descriptions ONLY. No abstract words.

Respond in exactly this structure:

PART 1 - JUMP IDENTIFICATION:
- Jump type and rotations from VIDEO 1
- What went wrong in VIDEO 1 — described as what the camera sees

PART 2 - VISUAL COMPARISON (VIDEO 1 vs VIDEO 2):
Approach / Takeoff / Air position / Landing — each with VIDEO 1 and VIDEO 2 descriptions

PART 3 - SCENE DETAILS FROM VIDEO 1:
- Costume, Rink, Camera angle

VIDEO_PROMPT_START
[4-6 sentences. Motion from VIDEO 2. Costume + rink from VIDEO 1.
Concrete visual terms only. No abstract words.]
VIDEO_PROMPT_END"""

USER_PROMPT_WITH_REFERENCE = """I am showing you two figure skating videos.
VIDEO 1 is the FAILED jump. VIDEO 2 is the CORRECT reference.
Compare them, note VIDEO 1's costume and rink, and write a VIDEO_PROMPT
showing VIDEO 2's motion with VIDEO 1's costume and rink setting.
End with VIDEO_PROMPT_START and VIDEO_PROMPT_END tags."""


# ---------------------------------------------------------------------------
# SSH / SCP helpers
# ---------------------------------------------------------------------------
def _ssh(config: PipelineConfig, command: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["ssh", f"{config.spark_user}@{config.spark_ip}", command],
        capture_output=True, text=True
    )

def _scp(src: str, dst: str) -> subprocess.CompletedProcess:
    return subprocess.run(["scp", src, dst], capture_output=True, text=True)

def _upload(config: PipelineConfig, local_path: Path, log: Callable) -> tuple[str, str]:
    """SCP a file to Spark videos folder. Returns (host_path, container_path)."""
    host      = f"{config.spark_videos_host}/{local_path.name}"
    container = f"{config.spark_videos_container}/{local_path.name}"
    size_kb   = local_path.stat().st_size // 1024
    log(f"Uploading {local_path.name} ({size_kb} KB)…")
    r = _scp(str(local_path), f"{config.spark_user}@{config.spark_ip}:{host}")
    if r.returncode != 0:
        raise RuntimeError(f"SCP failed: {r.stderr}")
    log(f"Uploaded → {host}")
    return host, container


# ---------------------------------------------------------------------------
# Step 1: Reason2 analysis
# ---------------------------------------------------------------------------
def analyze_jump(
    config: PipelineConfig,
    video_path: Path,
    reference_path: Path | None,
    log: Callable,
) -> str:
    _ssh(config, f"mkdir -p {config.spark_videos_host}")

    log("Uploading failed jump video…")
    _, container_original = _upload(config, video_path, log)

    if reference_path:
        log("Uploading reference video…")
        _, container_reference = _upload(config, reference_path, log)
        system_prompt = SYSTEM_PROMPT_WITH_REFERENCE
        user_content  = [
            {"type": "text",      "text": "VIDEO 1 — Failed jump:"},
            {"type": "video_url", "video_url": {"url": f"file://{container_original}"}},
            {"type": "text",      "text": "VIDEO 2 — Correct reference:"},
            {"type": "video_url", "video_url": {"url": f"file://{container_reference}"}},
            {"type": "text",      "text": USER_PROMPT_WITH_REFERENCE},
        ]
        log("Sending both videos to Reason2 for comparative analysis…")
    else:
        system_prompt = SYSTEM_PROMPT_SINGLE
        user_content  = [
            {"type": "video_url", "video_url": {"url": f"file://{container_original}"}},
            {"type": "text",      "text": USER_PROMPT_SINGLE},
        ]
        log("Sending video to Reason2 for analysis…")

    payload = {
        "model": "nvidia/Cosmos-Reason2-8B",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_content},
        ],
        "max_tokens": 2048,
        "temperature": 0.3,
    }

    resp = requests.post(
        f"http://{config.spark_ip}:8000/reason/v1/chat/completions",
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=300,
    )
    resp.raise_for_status()
    analysis = resp.json()["choices"][0]["message"]["content"]
    log("Reason2 analysis complete.")
    return analysis


# ---------------------------------------------------------------------------
# Step 2: Extract prompt
# ---------------------------------------------------------------------------
def extract_video_prompt(
    config: PipelineConfig,
    analysis: str,
    log: Callable,
) -> str:
    start_tag, end_tag = "VIDEO_PROMPT_START", "VIDEO_PROMPT_END"
    if start_tag in analysis and end_tag in analysis:
        s = analysis.index(start_tag) + len(start_tag)
        e = analysis.index(end_tag, s)
        prompt = analysis[s:e].strip()
        if prompt:
            log("Video prompt extracted from analysis.")
            return prompt

    log("Tags not found — asking Reason2 to generate prompt from analysis…")
    payload = {
        "model": "nvidia/Cosmos-Reason2-8B",
        "messages": [{"role": "user", "content": f"""
You analyzed a figure skating jump:
{analysis}

Write a 4-6 sentence video generation prompt describing what the camera sees
during the correctly executed version. Include costume and rink setting.
Use ONLY concrete visual terms. No abstract words.
Write ONLY the prompt."""}],
        "max_tokens": 400,
        "temperature": 0.2,
    }
    resp = requests.post(
        f"http://{config.spark_ip}:8000/reason/v1/chat/completions",
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=120,
    )
    resp.raise_for_status()
    prompt = resp.json()["choices"][0]["message"]["content"].strip()
    log("Fallback prompt generated.")
    return prompt


# ---------------------------------------------------------------------------
# Step 3: Fill placeholders
# ---------------------------------------------------------------------------
def fill_placeholders(
    config: PipelineConfig,
    prompt: str,
    analysis: str,
    log: Callable,
) -> str:
    if "[" not in prompt and "]" not in prompt:
        return prompt

    log("Filling placeholder brackets in prompt…")
    payload = {
        "model": "nvidia/Cosmos-Reason2-8B",
        "messages": [{"role": "user", "content": f"""
Prompt with brackets:
{prompt}

Analysis:
{analysis}

Replace ALL [bracketed] placeholders with specific visual details.
Remove all brackets. No abstract words. Write ONLY the final prompt."""}],
        "max_tokens": 400,
        "temperature": 0.2,
    }
    resp = requests.post(
        f"http://{config.spark_ip}:8000/reason/v1/chat/completions",
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=120,
    )
    resp.raise_for_status()
    filled = resp.json()["choices"][0]["message"]["content"].strip()
    log("Placeholders filled.")
    return filled


# ---------------------------------------------------------------------------
# Step 3.5: Translate abstract words to visual language
# ---------------------------------------------------------------------------
def make_prompt_visual(
    config: PipelineConfig,
    prompt: str,
    log: Callable,
) -> str:
    found = [w for w in ABSTRACT_WORDS if w.lower() in prompt.lower()]
    if not found:
        log("Step 3.5: Prompt already uses visual language — no changes needed.")
        return prompt

    log(f"Step 3.5: Translating abstract words: {found}")
    payload = {
        "model": "nvidia/Cosmos-Reason2-8B",
        "messages": [{"role": "user", "content": f"""
Rewrite this figure skating video prompt replacing every abstract phrase
with a concrete description of what a camera would literally see.

ORIGINAL:
{prompt}

TRANSLATION GUIDE:
- "powerful/strong takeoff" → "left toe pick stabs ice, right knee lifts until thigh is parallel to ice"
- "tight tuck" → "both arms pressed flat against chest forming an X, both ankles crossed at hip level"
- "knee drive" → "right knee rising until thigh is parallel to ice"
- "arm swing" → "both arms sweeping upward from hips to above head as toe pick contacts ice"
- "centered axis" → "body forming a straight vertical line from crown of head to blade"
- "free leg placement" → "left leg extending straight behind body at hip height"
- "knee absorption" → "right knee bending to 45 degrees as blade contacts ice"
- "clean rotation" → "body completing exactly 2.5 full spins"
- "flawless/perfect/correct/properly/successfully" → DELETE entirely
- "seamlessly/effectively/efficiently" → DELETE entirely

Keep costume and rink descriptions unchanged.
Write ONLY the rewritten prompt."""}],
        "max_tokens": 500,
        "temperature": 0.1,
    }
    resp = requests.post(
        f"http://{config.spark_ip}:8000/reason/v1/chat/completions",
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=120,
    )
    resp.raise_for_status()
    visual = resp.json()["choices"][0]["message"]["content"].strip()
    still = [w for w in ABSTRACT_WORDS if w.lower() in visual.lower()]
    if still:
        log(f"Warning: some abstract words remain: {still}")
    else:
        log("All abstract words replaced with visual descriptions.")
    return visual


# ---------------------------------------------------------------------------
# Step 4: Generate video with Predict2.5
# ---------------------------------------------------------------------------
def generate_video(
    config: PipelineConfig,
    prompt: str,
    output_name: str,
    input_video: Path,
    log: Callable,
) -> None:
    spark_input_host      = f"{config.spark_inputs_host}/{input_video.name}"
    spark_input_container = f"{config.spark_inputs_container}/{input_video.name}"

    log(f"Uploading input video to Spark inputs folder…")
    r = _scp(str(input_video), f"{config.spark_user}@{config.spark_ip}:{spark_input_host}")
    if r.returncode != 0:
        raise RuntimeError(f"SCP failed: {r.stderr}")
    log(f"Input video ready at {spark_input_container}")

    inference = {
        "inference_type": "video2world",
        "name": output_name,
        "prompt": prompt,
        "input_path": spark_input_container,
        "seed": 42,
        "num_steps": 35,
        "guidance": 7,
    }
    json_path = config.local_base / "pending.json"
    json_path.write_text(json.dumps(inference, indent=2))

    r = _scp(
        str(json_path),
        f"{config.spark_user}@{config.spark_ip}:{config.spark_inputs_host}/pending.json",
    )
    if r.returncode != 0:
        raise RuntimeError(f"SCP request failed: {r.stderr}")
    log("Inference request sent to Predict2.5.")


# ---------------------------------------------------------------------------
# Step 5: Poll for output
# ---------------------------------------------------------------------------
def poll_for_output(
    config: PipelineConfig,
    output_name: str,
    log: Callable,
    progress: Callable[[int], None] | None = None,
) -> Path:
    remote_mp4  = f"{config.spark_outputs_host}/{output_name}.mp4"
    remote_json = f"{config.spark_outputs_host}/{output_name}.json"
    local       = config.outputs_dir / f"{output_name}.mp4"

    # Predict2.5 writes the .json sidecar at the START of generation and the
    # .mp4 at the END.  We can't delete stale root-owned files, so instead we
    # wait until BOTH conditions are true:
    #   1. The .json has mtime > submit_epoch  (our job was picked up)
    #   2. The .mp4  has mtime >= .json mtime   (generation finished)
    ts_result = _ssh(config, "date +%s")
    submit_epoch = int(ts_result.stdout.strip()) if ts_result.returncode == 0 else int(time.time())
    log("Polling for new output (ignoring stale files)…")

    start    = time.time()
    max_wait = 7200
    interval = 30

    while time.time() - start < max_wait:
        elapsed = int(time.time() - start)

        # Get mtimes for both the sidecar JSON and the MP4
        check = _ssh(
            config,
            f'echo "$(stat -c %Y {remote_json} 2>/dev/null || echo 0)'
            f' $(stat -c %Y {remote_mp4} 2>/dev/null || echo 0)"',
        )
        parts = check.stdout.strip().split()
        json_mtime = int(parts[0]) if len(parts) >= 1 else 0
        mp4_mtime  = int(parts[1]) if len(parts) >= 2 else 0

        if json_mtime > submit_epoch and mp4_mtime >= json_mtime:
            log(f"Video ready after {elapsed//60}m {elapsed%60}s — copying back…")
            r = _scp(
                f"{config.spark_user}@{config.spark_ip}:{remote_mp4}",
                str(local),
            )
            if r.returncode != 0:
                raise RuntimeError(f"SCP back failed: {r.stderr}")
            log(f"Saved to {local}")
            return local

        pct = min(95, int((elapsed / max_wait) * 100))
        if progress:
            progress(pct)
        log(f"Generating… {elapsed//60}m {elapsed%60}s elapsed")
        time.sleep(interval)

    raise TimeoutError("Video generation timed out after 2 hours.")


# ---------------------------------------------------------------------------
# Full pipeline — called by both CLI and API
# ---------------------------------------------------------------------------
def run_pipeline(
    config: PipelineConfig,
    video_path: Path,
    reference_path: Path | None,
    log: Callable[[str], None],
    progress: Callable[[int], None] | None = None,
    generate_video_flag: bool = True,
) -> AnalysisResult:
    output_name = f"corrected_{video_path.stem}"

    config.outputs_dir.mkdir(parents=True, exist_ok=True)
    config.analysis_dir.mkdir(parents=True, exist_ok=True)

    # Step 1
    log("=== Step 1: Analyzing jump with Reason2 ===")
    if progress: progress(5)
    analysis = analyze_jump(config, video_path, reference_path, log)

    analysis_file = config.analysis_dir / f"{output_name}_analysis.txt"
    analysis_file.write_text(analysis)
    log(f"Analysis saved → {analysis_file}")

    # Step 2
    log("=== Step 2: Extracting video prompt ===")
    if progress: progress(20)
    prompt = extract_video_prompt(config, analysis, log)

    # Step 3
    log("=== Step 3: Filling placeholders ===")
    if progress: progress(30)
    prompt = fill_placeholders(config, prompt, analysis, log)

    # Step 3.5
    log("=== Step 3.5: Ensuring visual language ===")
    if progress: progress(40)
    prompt = make_prompt_visual(config, prompt, log)

    prompt_file = config.analysis_dir / f"{output_name}_prompt.txt"
    prompt_file.write_text(prompt)
    log(f"Final prompt saved → {prompt_file}")

    if not generate_video_flag:
        log("=== Skipping video generation (analysis only) ===")
        if progress: progress(100)
        return AnalysisResult(
            output_name=output_name,
            analysis=analysis,
            prompt=prompt,
            output_video=None,
            analysis_file=analysis_file,
            prompt_file=prompt_file,
        )

    # Step 4
    log("=== Step 4: Generating corrected video with Predict2.5 ===")
    if progress: progress(45)
    input_video = reference_path if reference_path else video_path
    generate_video(config, prompt, output_name, input_video, log)

    # Step 5
    log("=== Step 5: Waiting for Predict2.5 output (45-70 min) ===")
    if progress: progress(50)
    output_path = poll_for_output(config, output_name, log, progress)
    if progress: progress(100)

    return AnalysisResult(
        output_name=output_name,
        analysis=analysis,
        prompt=prompt,
        output_video=output_path,
        analysis_file=analysis_file,
        prompt_file=prompt_file,
    )


# ---------------------------------------------------------------------------
# Connectivity check
# ---------------------------------------------------------------------------
def check_spark(config: PipelineConfig) -> dict:
    result = {"gateway": False, "reason2": False, "model": None, "error": None}
    try:
        r = requests.get(f"http://{config.spark_ip}:8000/health", timeout=10)
        result["gateway"] = r.status_code == 200
        r2 = requests.get(f"http://{config.spark_ip}:8000/reason/v1/models", timeout=10)
        if r2.status_code == 200:
            result["reason2"] = True
            result["model"]   = r2.json()["data"][0]["id"]
    except Exception as e:
        result["error"] = str(e)
    return result
