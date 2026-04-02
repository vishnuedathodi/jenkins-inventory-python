# Jenkins Pipeline Inventory

Automated inventory tool that scans **all Jenkins pipelines** and exports metadata to a CSV file.
Designed for scale (100 k+ pipelines) and runs as a **GitHub Actions** workflow.

---

## 📋 Output Columns

| Column | Source | Description |
|---|---|---|
| `pipeline_name` | URL | Job name (last segment of the job path) |
| `pipeline_url` | API | Direct URL to the Jenkins job |
| `folder_path` | URL | Full folder hierarchy, e.g. `TeamA » ServiceB » SubFolder` |
| `jenkinsfile_repo` | `config.xml` | Git remote URL of the Jenkinsfile repo |
| `jenkinsfile_path` | `config.xml` | Path to the Jenkinsfile (default: `Jenkinsfile`) |
| `jenkinsfile_branch` | `config.xml` | SCM branch configured for the pipeline |
| `pipeline_type` | `config.xml` | Pipeline / Multibranch / Freestyle / Matrix |
| `shared_libraries` | `config.xml` → console | Semicolon-separated list of shared libraries |
| `tech_stack` | `config.xml` → console | Detected tech stack (Maven, Docker, k8s, …) |
| `last_run_date` | API | Timestamp of the last build (`YYYY-MM-DD HH:MM:SS` UTC) |
| `source` | internal | Where data was sourced from |
| `error` | internal | Any error message for this job |

---

## 🏗️ Architecture

```
main()
  └─ collect_all_jobs()        # Recursive BFS walk (folders, orgs, multibranch)
       └─ ThreadPoolExecutor   # MAX_WORKERS concurrent threads per batch
            └─ process_job()   # Per-job worker (thread-safe session)
                 ├─ GET config.xml      → parse_config_xml()
                 ├─ GET consoleText     → parse_console_output()  [fallback]
                 └─ GET lastBuild/api   → last_run_date
  └─ write_csv()               # Single-writer, streaming output
```

**Crash-prevention strategies:**
- Jobs processed in configurable **batches** (`BATCH_SIZE=500`)
- Per-request **retry with exponential back-off** (3 retries, 2× backoff)
- Per-thread **rate-limiting delay** (`RATE_LIMIT_DELAY=0.05 s`)
- `config.xml` is the **primary** source; console output is only fetched when fields are missing
- All exceptions are caught per-job — one failure never stops the rest
- Progress logged every 1 000 jobs

---

## 🚀 Running via GitHub Actions

### Prerequisites

Add these **repository secrets** (Settings → Secrets and variables → Actions):

| Secret | Value |
|---|---|
| `JENKINS_URL` | `https://your-jenkins.example.com` |
| `JENKINS_USER` | Jenkins username / service account |
| `JENKINS_TOKEN` | Jenkins API token |

### Trigger

Go to **Actions → Jenkins Pipeline Inventory → Run workflow**.

Optional inputs:
- `max_workers` – number of parallel threads (default `20`)
- `batch_size` – jobs per batch (default `500`)
- `output_csv` – CSV filename (default `jenkins_pipeline_inventory.csv`)

The CSV and log are uploaded as a **workflow artifact** (retained 30 days).

---

## 💻 Running Locally

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set environment variables
$env:JENKINS_URL   = "https://your-jenkins.example.com"
$env:JENKINS_USER  = "your-username"
$env:JENKINS_TOKEN = "your-api-token"

# Optional tuning
$env:MAX_WORKERS  = "30"
$env:BATCH_SIZE   = "200"
$env:OUTPUT_CSV   = "my_inventory.csv"

# 3. Run
python jenkins_inventory.py
```

---

## ⚙️ Environment Variables

| Variable | Default | Description |
|---|---|---|
| `JENKINS_URL` | *(required)* | Base URL of Jenkins |
| `JENKINS_USER` | *(required)* | Jenkins username |
| `JENKINS_TOKEN` | *(required)* | Jenkins API token |
| `OUTPUT_CSV` | `jenkins_pipeline_inventory.csv` | Output file name |
| `MAX_WORKERS` | `20` | Parallel threads per batch |
| `BATCH_SIZE` | `500` | Jobs per batch |
| `REQUEST_TIMEOUT` | `30` | HTTP timeout (seconds) |
| `RETRY_COUNT` | `3` | Retries on transient errors |
| `BACKOFF_FACTOR` | `2.0` | Exponential back-off factor |
| `RATE_LIMIT_DELAY` | `0.05` | Delay between requests per thread (seconds) |

---

## 🔍 Tech Stack Detection

The script scans both `config.xml` content and console output for keywords:

`Maven` · `Gradle` · `npm` · `Python` · `Docker` · `Kubernetes` · `Terraform` · `Ansible` · `.NET` · `Java` · `Go` · `Ruby` · `Scala`

---

## 📁 Project Structure

```
.
├── jenkins_inventory.py          # Main script
├── requirements.txt              # Python dependencies
├── README.md
└── .github/
    └── workflows/
        └── jenkins_inventory.yml # GitHub Actions workflow
```
