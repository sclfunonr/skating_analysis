# Skating Pipeline

AI-powered figure skating jump analysis and correction.
Repository: `https://github.com/sclfunonr/skating_analysis`

## Quickstart

```bash
git clone https://github.com/sclfunonr/skating_analysis.git
cd skating_analysis
uv sync
uv run skating-api
```

Then open `http://localhost:7860/`.

This project has:
- A FastAPI backend in `service/`
- A single-page web app in `webapp/index.html` (served by the backend at `/`)
- A pipeline that:
1. Analyzes a failed jump with Cosmos-Reason2
2. Builds a visual-only correction prompt
3. Optionally generates a corrected video with Cosmos-Predict2.5

## Prerequisites

- Linux or macOS shell
- Python 3.12+
- `uv` installed
- Network access to your Spark host
- SSH + SCP access to the Spark host user (key-based auth recommended)

Default Spark config in code:
- `spark_ip`: `192.168.1.125` - replace it with your LLMs server IP
- `spark_user`: `suetchun` - replace it with your LLMs server user name.

You can change these in the UI settings or via `POST /config`.

## Setup

From project root:

```bash
git clone https://github.com/sclfunonr/skating_analysis.git
cd skating_analysis
uv sync
```

Notes:
- Do not run `uv init` for this repo (it is already initialized).
- `uv sync` creates/updates `.venv` and installs dependencies.

## Run

### Option A: Use the project script

```bash
uv run skating-api
```

Optional host/port override:

```bash
SKATING_API_HOST=127.0.0.1 SKATING_API_PORT=8000 uv run skating-api
```

### Option B: Run uvicorn directly

```bash
uv run uvicorn service.api:app --host 0.0.0.0 --port 7860 --reload
```

Important: include `:app` in `service.api:app`.

## Open the app

- Web UI: `http://localhost:7860/`
- Health check: `http://localhost:7860/health`

## Demo Walkthrough (What To Look For)

### 1) Confirm Spark connectivity

In the top-right status pill and Spark Status card:
- `Gateway: Online`
- `Reason2: Ready`
- Model name is populated

If not ready, open settings and update Spark IP/user.

### 2) Submit a new session

In `New Session`:
- Upload a failed jump video (required)
- Upload a correct reference video of the same jump type (optional but recommended)
- Choose one mode:
  - `Generate corrected video` ON: full pipeline (can take 30-70+ minutes)
  - OFF: analysis/prompt only (faster, around minutes)

Click submit.

### 3) Watch live progress

In `Session` view, watch:
- Progress bar and step updates
- Live processing log stream (SSE)
- Session status moves from `pending` -> `running` -> `done` (or `failed`)

Expected major steps in logs:
1. Analyze jump with Reason2
2. Extract/fill prompt
3. Translate abstract wording to visual language
4. Generate with Predict2.5 (if enabled)
5. Poll and download output video

### 4) Validate outputs

On completion:
- Result panel appears with generated video (if enabled)
- Download link for `/jobs/{id}/video` works
- Analysis and final prompt text are present in job result

Also on disk (default local base `~/skating_project`):
- `analysis/` contains `*_analysis.txt` and `*_prompt.txt`
- `outputs/` contains generated `*.mp4` files

## Useful API Endpoints

- `GET /health` - service + Spark/Reason2 availability
- `POST /config` - update `spark_ip`, `spark_user` (form data)
- `POST /jobs` - submit job (`video`, optional `reference`, `generate_video`)
- `GET /jobs` - list jobs
- `GET /jobs/{id}` - full job details + logs
- `GET /jobs/{id}/stream` - live SSE updates
- `GET /jobs/{id}/video` - download generated video

## Quick API Smoke Test

```bash
curl http://localhost:7860/health
curl http://localhost:7860/jobs
```

If `/jobs` returns `[]` and HTTP 200, the API is running correctly.

## Troubleshooting

- `Import string ... must be in format "<module>:<attribute>"`:
  - Use `service.api:app`, not `service.api`.

- `FastAPI.__call__ missing scope/receive/send` when running `skating-api`:
  - Run `uv sync` after updating project scripts so entrypoints are regenerated.

- Spark shows offline:
  - Verify Spark IP/user, SSH access, and that Reason2 endpoint on port `8000` is reachable.

- No `GET /jobs` access logs:
  - This is normal until a client actually requests `/jobs`.
