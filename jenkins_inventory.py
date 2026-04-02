"""
Jenkins Pipeline Inventory Script
===================================
Fetches pipeline metadata for 100,000+ pipelines using:
- config.xml as primary data source
- Console output as fallback
- ThreadPoolExecutor for concurrency
- Rate-limiting & retry logic to avoid crashing Jenkins
- Output: CSV file
"""

import os
import re
import csv
import time
import logging
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, fields, astuple
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("jenkins_inventory.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration  (all values can be overridden via environment variables)
# ---------------------------------------------------------------------------
JENKINS_URL        = os.environ.get("JENKINS_URL", "https://jenkins.example.com").rstrip("/")
JENKINS_USER       = os.environ.get("JENKINS_USER", "")
JENKINS_TOKEN      = os.environ.get("JENKINS_TOKEN", "")
OUTPUT_CSV         = os.environ.get("OUTPUT_CSV", "jenkins_pipeline_inventory.csv")
MAX_WORKERS        = int(os.environ.get("MAX_WORKERS", "20"))       # concurrent threads
BATCH_SIZE         = int(os.environ.get("BATCH_SIZE", "500"))       # jobs per batch
REQUEST_TIMEOUT    = int(os.environ.get("REQUEST_TIMEOUT", "30"))   # seconds
RETRY_COUNT        = int(os.environ.get("RETRY_COUNT", "3"))
BACKOFF_FACTOR     = float(os.environ.get("BACKOFF_FACTOR", "2.0"))
RATE_LIMIT_DELAY   = float(os.environ.get("RATE_LIMIT_DELAY", "0.05"))  # seconds between requests per thread

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class PipelineRecord:
    pipeline_name:      str = ""
    pipeline_url:       str = ""
    folder_path:        str = ""   # full folder hierarchy, e.g. "TeamA » ServiceB"
    jenkinsfile_repo:   str = ""
    jenkinsfile_path:   str = ""
    jenkinsfile_branch: str = ""
    pipeline_type:      str = ""
    shared_libraries:   str = ""
    tech_stack:         str = ""
    last_run_date:      str = ""
    source:             str = ""   # "config.xml" | "console" | "partial"
    error:              str = ""

CSV_HEADERS = [f.name for f in fields(PipelineRecord)]

# ---------------------------------------------------------------------------
# HTTP Session factory (per-thread sessions for thread-safety)
# ---------------------------------------------------------------------------
def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=RETRY_COUNT,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    if JENKINS_USER and JENKINS_TOKEN:
        session.auth = (JENKINS_USER, JENKINS_TOKEN)
    session.headers.update({"Accept": "application/json"})
    return session


# ---------------------------------------------------------------------------
# Jenkins API helpers
# ---------------------------------------------------------------------------
def get_json(session: requests.Session, url: str) -> Optional[dict]:
    """GET a Jenkins JSON API endpoint, return parsed dict or None."""
    try:
        time.sleep(RATE_LIMIT_DELAY)
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
        logger.warning("GET %s -> HTTP %s", url, resp.status_code)
    except Exception as exc:
        logger.warning("GET %s failed: %s", url, exc)
    return None


def get_text(session: requests.Session, url: str) -> Optional[str]:
    """GET a plain-text or XML endpoint, return text or None."""
    try:
        time.sleep(RATE_LIMIT_DELAY)
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
        logger.warning("GET %s -> HTTP %s", url, resp.status_code)
    except Exception as exc:
        logger.warning("GET %s failed: %s", url, exc)
    return None


# ---------------------------------------------------------------------------
# Folder path helper
# ---------------------------------------------------------------------------
def _derive_folder_path(job_url: str) -> str:
    """
    Build a human-readable folder hierarchy from a Jenkins job URL.

    Example URL:
      https://jenkins.example.com/job/TeamA/job/ServiceB/job/deploy
    Returns:
      "TeamA » ServiceB"   (last segment is the job name itself, so excluded)
    """
    # Strip base URL, split on /job/, drop empty parts
    relative = job_url.replace(JENKINS_URL, "").strip("/")
    segments = [s for s in relative.split("/job/") if s]
    # Everything except the last segment (which is the job name) is the folder
    folder_parts = segments[:-1]
    return " » ".join(folder_parts) if folder_parts else "(root)"


# ---------------------------------------------------------------------------
# Collect ALL job URLs from Jenkins (supports folders / multi-branch)
# ---------------------------------------------------------------------------

# Each queue entry is a tuple: (url, folder_label)
# folder_label tracks the human-readable path of the *parent* container.
def collect_all_jobs(session: requests.Session) -> list[tuple[str, str]]:
    """
    Recursively walk the Jenkins job tree and return a flat list of
    (job_url, folder_path) tuples for every pipeline/freestyle/multibranch job.
    Uses tree= parameter to keep API payloads small.
    """
    logger.info("Collecting all jobs from Jenkins …")
    # Queue items: (base_url, folder_label_so_far)
    all_jobs: list[tuple[str, str]] = []
    queue: list[tuple[str, str]] = [(JENKINS_URL, "")]

    while queue:
        base, parent_folder = queue.pop(0)
        api_url = f"{base}/api/json?tree=jobs[name,url,_class,jobs[name,url,_class]]"
        data = get_json(session, api_url)
        if not data:
            continue
        for job in data.get("jobs", []):
            job_url   = job.get("url", "").rstrip("/")
            job_name  = job.get("name", "")
            job_class = job.get("_class", "")
            # Build the folder label for items *inside* this node
            current_folder = f"{parent_folder} » {job_name}".lstrip(" » ")

            if any(k in job_class for k in ["Folder", "Organization"]):
                # Pure container — recurse, don't emit a job record
                queue.append((job_url, current_folder))
            elif "WorkflowMultiBranchProject" in job_class:
                # Multibranch parent — recurse to get individual branch jobs
                # but also record the parent itself (folder = parent_folder)
                all_jobs.append((job_url, parent_folder if parent_folder else "(root)"))
                queue.append((job_url, current_folder))
            else:
                # Leaf job — record with its folder
                folder = parent_folder if parent_folder else "(root)"
                all_jobs.append((job_url, folder))

    logger.info("Total jobs found: %d", len(all_jobs))
    return all_jobs


# ---------------------------------------------------------------------------
# Parse config.xml
# ---------------------------------------------------------------------------
_NS = {
    "flow": "flow-definition",
}

def _xml_text(root: ET.Element, *xpaths: str) -> str:
    """Try multiple XPaths, return first non-empty text found."""
    for xpath in xpaths:
        el = root.find(xpath)
        if el is not None and el.text:
            return el.text.strip()
    return ""


def parse_config_xml(xml_text: str, job_url: str) -> PipelineRecord:
    """
    Extract all metadata from config.xml.
    Handles:
      - WorkflowJob (Scripted / Declarative Pipeline)
      - WorkflowMultiBranchProject
      - FreeStyleProject
      - MatrixProject
    """
    rec = PipelineRecord(pipeline_url=job_url)
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        rec.error = f"XML parse error: {exc}"
        return rec

    tag = root.tag  # e.g. flow-definition, project, WorkflowMultiBranchProject

    # ── Pipeline type ───────────────────────────────────────────────────────
    type_map = {
        "flow-definition":              "Pipeline (Scripted/Declarative)",
        "org.jenkinsci.plugins.workflow.job.WorkflowJob": "Pipeline (Scripted/Declarative)",
        "org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject": "Multibranch Pipeline",
        "project":                      "Freestyle",
        "matrix-project":               "Matrix",
        "com.tikal.jenkins.plugins.multijob.MultiJobProject": "MultiJob",
    }
    rec.pipeline_type = type_map.get(tag, tag)

    # ── SCM block (works for Pipeline Script from SCM + Multibranch) ────────
    # --- Git SCM ---
    git_url    = _xml_text(root, ".//scm/userRemoteConfigs/hudson.plugins.git.UserRemoteConfig/url",
                                  ".//sources/data/jenkins.branch.BranchSource/source/remote",
                                  ".//scm/remote")
    git_branch = _xml_text(root, ".//scm/branches/hudson.plugins.git.BranchSpec/name",
                                  ".//sources/data/jenkins.branch.BranchSource/source/credentialsId",
                                  ".//scm/branch")
    # --- GitHub / BitBucket source ---
    gh_repo    = _xml_text(root, ".//sources/data/jenkins.branch.BranchSource/source/repoOwner",
                                  ".//sources/data/jenkins.branch.BranchSource/source/repository")
    gh_server  = _xml_text(root, ".//sources/data/jenkins.branch.BranchSource/source/serverUrl",
                                  ".//sources/data/jenkins.branch.BranchSource/source/apiUri")

    # Build repo URL
    if git_url:
        rec.jenkinsfile_repo = git_url
    elif gh_server and gh_repo:
        rec.jenkinsfile_repo = f"{gh_server}/{gh_repo}"

    # Branch
    rec.jenkinsfile_branch = git_branch or _xml_text(
        root,
        ".//sources/data/jenkins.branch.BranchSource/source/includes",
    )

    # Jenkinsfile path (scriptPath)
    rec.jenkinsfile_path = _xml_text(
        root,
        ".//definition/scriptPath",
        ".//scriptPath",
        ".//factory/scriptPath",
    )
    if not rec.jenkinsfile_path:
        rec.jenkinsfile_path = "Jenkinsfile"   # default

    # Inline script (not from SCM)
    inline_script = _xml_text(root, ".//definition/script", ".//script")

    # ── Shared Libraries ────────────────────────────────────────────────────
    libs = []
    for lib_el in root.findall(".//libraries/org.jenkinsci.plugins.workflow.libs.LibraryConfiguration"):
        lib_name = _xml_text(lib_el, "name")
        if lib_name:
            libs.append(lib_name)
    # Also check @Library annotations in inline scripts
    if inline_script:
        libs += re.findall(r"@Library\(['\"]([^'\"]+)['\"]\)", inline_script)
    rec.shared_libraries = "; ".join(sorted(set(libs))) if libs else ""

    # ── Tech Stack detection ─────────────────────────────────────────────────
    rec.tech_stack = detect_tech_stack(root, inline_script or "")

    rec.source = "config.xml"
    return rec


# ---------------------------------------------------------------------------
# Tech-stack heuristics
# ---------------------------------------------------------------------------
TECH_KEYWORDS: dict[str, list[str]] = {
    "Maven":      ["mvn ", "maven", "POM", "pom.xml"],
    "Gradle":     ["gradle", "gradlew"],
    "npm":        ["npm ", "node", "package.json"],
    "Python":     ["pip ", "python", "pytest", "tox"],
    "Docker":     ["docker", "Dockerfile", "containerize"],
    "Kubernetes": ["kubectl", "helm", "k8s", "kubernetes"],
    "Terraform":  ["terraform", "tf "],
    "Ansible":    ["ansible", "playbook"],
    ".NET":       ["dotnet", "msbuild", "nuget"],
    "Java":       ["java ", "javac", "jdk"],
    "Go":         ["go build", "go test"],
    "Ruby":       ["gem ", "bundle exec", "rake"],
    "Scala":      ["sbt ", "scala"],
}

def detect_tech_stack(root: ET.Element, script_text: str) -> str:
    """Guess tech stack from builders, shell steps, and inline script text."""
    combined = script_text.lower()
    # Pull all text out of XML for keyword scanning
    for el in root.iter():
        if el.text:
            combined += " " + el.text.lower()

    found = [tech for tech, kws in TECH_KEYWORDS.items()
             if any(kw.lower() in combined for kw in kws)]
    return "; ".join(found) if found else "Unknown"


# ---------------------------------------------------------------------------
# Fallback: console output parsing
# ---------------------------------------------------------------------------
def parse_console_output(console_text: str, rec: PipelineRecord) -> PipelineRecord:
    """Fill missing fields from last build console output."""
    if not rec.jenkinsfile_repo:
        # Try to find git clone / checkout URLs
        m = re.search(r"(?:Cloning|Fetching|checkout)\s+(?:the\s+)?(?:repository|repo)?\s*['\"]?(https?://[^\s'\"]+)", console_text, re.I)
        if m:
            rec.jenkinsfile_repo = m.group(1)

    if not rec.jenkinsfile_branch:
        m = re.search(r"Checking out\s+(?:Revision\s+\S+\s+)?\((.+?)\)", console_text)
        if m:
            rec.jenkinsfile_branch = m.group(1).strip()

    if not rec.shared_libraries:
        libs = re.findall(r"Loading library\s+([^\s@]+)@", console_text, re.I)
        rec.shared_libraries = "; ".join(sorted(set(libs))) if libs else rec.shared_libraries

    if not rec.tech_stack or rec.tech_stack == "Unknown":
        found = [tech for tech, kws in TECH_KEYWORDS.items()
                 if any(kw.lower() in console_text.lower() for kw in kws)]
        if found:
            rec.tech_stack = "; ".join(found)

    rec.source = "config.xml+console" if rec.source == "config.xml" else "console"
    return rec


# ---------------------------------------------------------------------------
# Last-run date
# ---------------------------------------------------------------------------
def get_last_run_date(session: requests.Session, job_url: str) -> str:
    data = get_json(session, f"{job_url}/lastBuild/api/json?tree=timestamp")
    if data and "timestamp" in data:
        ts = data["timestamp"] / 1000
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts))
    return ""


# ---------------------------------------------------------------------------
# Main per-job worker
# ---------------------------------------------------------------------------
def process_job(job_url: str, folder_path: str) -> PipelineRecord:
    """
    Full pipeline for a single job:
    1. Fetch config.xml  → parse
    2. If key fields missing → fetch console output → parse
    3. Fetch last-run date
    """
    session = make_session()   # thread-local session
    rec = PipelineRecord(pipeline_url=job_url)

    # Derive a display name from the URL (last /job/<name> segment)
    rec.pipeline_name = job_url.rstrip("/").split("/job/")[-1]
    rec.folder_path   = folder_path

    # ── Step 1: config.xml ──────────────────────────────────────────────────
    config_text = get_text(session, f"{job_url}/config.xml")
    if config_text:
        rec = parse_config_xml(config_text, job_url)
        rec.pipeline_name = job_url.rstrip("/").split("/job/")[-1]
        rec.pipeline_url  = job_url
        rec.folder_path   = folder_path
    else:
        rec.error  = "config.xml unavailable"
        rec.source = "none"

    # ── Step 2: console fallback if needed ──────────────────────────────────
    needs_fallback = (
        not rec.jenkinsfile_repo
        or not rec.jenkinsfile_branch
        or not rec.shared_libraries
        or rec.tech_stack in ("", "Unknown")
    )
    if needs_fallback:
        console_text = get_text(session, f"{job_url}/lastBuild/consoleText")
        if console_text:
            rec = parse_console_output(console_text, rec)

    # ── Step 3: last run date ────────────────────────────────────────────────
    rec.last_run_date = get_last_run_date(session, job_url)

    return rec


# ---------------------------------------------------------------------------
# Batch processor with progress logging
# ---------------------------------------------------------------------------
def process_jobs_in_batches(job_entries: list[tuple[str, str]]) -> list[PipelineRecord]:
    results: list[PipelineRecord] = []
    total = len(job_entries)
    processed = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = job_entries[batch_start: batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        logger.info("Processing batch %d (%d – %d of %d) …",
                    batch_num, batch_start + 1, batch_start + len(batch), total)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS,
                                thread_name_prefix="jenkins-worker") as executor:
            future_map = {
                executor.submit(process_job, url, folder): (url, folder)
                for url, folder in batch
            }
            for future in as_completed(future_map):
                url, folder = future_map[future]
                try:
                    rec = future.result()
                except Exception as exc:
                    logger.error("Unhandled error for %s: %s", url, exc)
                    rec = PipelineRecord(pipeline_url=url, folder_path=folder, error=str(exc))
                results.append(rec)
                processed += 1
                if processed % 1000 == 0:
                    logger.info("Progress: %d / %d jobs processed (%.1f%%)",
                                processed, total, 100 * processed / total)

    return results


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------
def write_csv(records: list[PipelineRecord], path: str) -> None:
    logger.info("Writing %d records to %s …", len(records), path)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_HEADERS)
        for rec in records:
            writer.writerow(astuple(rec))
    logger.info("CSV written: %s", path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("Jenkins Inventory starting …")
    logger.info("Target Jenkins: %s", JENKINS_URL)
    logger.info("Workers: %d  |  Batch size: %d", MAX_WORKERS, BATCH_SIZE)

    if not JENKINS_URL or JENKINS_URL == "https://jenkins.example.com":
        logger.error("JENKINS_URL is not set. Export the environment variable and retry.")
        raise SystemExit(1)

    session = make_session()
    job_entries = collect_all_jobs(session)

    if not job_entries:
        logger.warning("No jobs found. Check credentials and JENKINS_URL.")
        return

    records = process_jobs_in_batches(job_entries)
    write_csv(records, OUTPUT_CSV)

    ok    = sum(1 for r in records if not r.error)
    err   = sum(1 for r in records if r.error)
    logger.info("Done. Success: %d  |  Errors: %d  |  Total: %d", ok, err, len(records))


if __name__ == "__main__":
    main()
