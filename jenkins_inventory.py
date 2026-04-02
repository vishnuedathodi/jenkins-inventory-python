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
JENKINS_URL          = os.environ.get("JENKINS_URL", "https://jenkins.example.com").rstrip("/")
JENKINS_USER         = os.environ.get("JENKINS_USER", "")
JENKINS_TOKEN        = os.environ.get("JENKINS_TOKEN", "")
OUTPUT_CSV           = os.environ.get("OUTPUT_CSV", "jenkins_pipeline_inventory.csv")
MAX_WORKERS          = int(os.environ.get("MAX_WORKERS", "20"))        # concurrent threads
BATCH_SIZE           = int(os.environ.get("BATCH_SIZE", "500"))        # jobs per batch
CONNECT_TIMEOUT      = int(os.environ.get("CONNECT_TIMEOUT", "10"))    # TCP connect timeout (s)
READ_TIMEOUT         = int(os.environ.get("READ_TIMEOUT", "60"))       # response read timeout (s)
RETRY_COUNT          = int(os.environ.get("RETRY_COUNT", "3"))
BACKOFF_FACTOR       = float(os.environ.get("BACKOFF_FACTOR", "2.0"))
RATE_LIMIT_DELAY     = float(os.environ.get("RATE_LIMIT_DELAY", "0.05"))  # seconds between requests per thread
VERIFY_SSL           = os.environ.get("VERIFY_SSL", "true").lower() != "false"  # set VERIFY_SSL=false for self-signed certs

# Convenience tuple used in every request call
REQUEST_TIMEOUT      = (CONNECT_TIMEOUT, READ_TIMEOUT)

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
    session.verify = VERIFY_SSL
    if not VERIFY_SSL:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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

    NOTE: We do NOT use tree= depth limiting because it silently drops nested
    folders beyond the requested depth (which caused the "only 4 entries" bug).
    Instead we fetch one level at a time and BFS-recurse explicitly.
    """
    logger.info("Collecting all jobs from Jenkins …")
    all_jobs: list[tuple[str, str]] = []
    # Queue items: (base_url, folder_label_so_far)
    queue: list[tuple[str, str]] = [(JENKINS_URL, "")]
    visited: set[str] = set()

    while queue:
        base, parent_folder = queue.pop(0)
        if base in visited:
            continue
        visited.add(base)

        # Fetch ONLY one level — no nested tree= so nothing gets silently truncated
        api_url = f"{base}/api/json?tree=jobs[name,url,_class]"
        data = get_json(session, api_url)
        if not data:
            logger.warning("No data returned for %s — skipping", base)
            continue

        for job in data.get("jobs", []):
            job_url   = job.get("url", "").rstrip("/")
            job_name  = job.get("name", "")
            job_class = job.get("_class", "")

            if not job_url:
                continue

            # Build the folder label for items *inside* this node
            current_label = f"{parent_folder} » {job_name}".lstrip(" » ")

            if any(k in job_class for k in ("Folder", "OrganizationFolder")):
                # Pure container — recurse, no job record emitted
                queue.append((job_url, current_label))

            elif "WorkflowMultiBranchProject" in job_class:
                # Multibranch parent: record it AND recurse into branch children
                folder = parent_folder if parent_folder else "(root)"
                all_jobs.append((job_url, folder))
                queue.append((job_url, current_label))

            else:
                # Leaf job (Pipeline, Freestyle, Matrix, etc.)
                folder = parent_folder if parent_folder else "(root)"
                all_jobs.append((job_url, folder))

        logger.debug("BFS queue size: %d  |  collected so far: %d", len(queue), len(all_jobs))

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


def _clean_branch(raw: str) -> str:
    """
    Normalise branch refs returned by Jenkins config.xml.
    Examples:
      refs/heads/main   → main
      origin/main       → main
      */main            → main
      main              → main
    """
    raw = raw.strip()
    for prefix in ("refs/heads/", "refs/remotes/", "origin/", "*/"):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
    return raw


def parse_config_xml(xml_text: str, job_url: str) -> PipelineRecord:
    """
    Extract SCM metadata from config.xml.
    NOTE: tech_stack and shared_libraries are intentionally left blank here —
    they are populated from console output in process_job() for higher accuracy.
    """
    rec = PipelineRecord(pipeline_url=job_url)
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        rec.error = f"XML parse error: {exc}"
        return rec

    tag = root.tag

    # ── Pipeline type ────────────────────────────────────────────────────────
    type_map = {
        "flow-definition":              "Pipeline (Scripted/Declarative)",
        "org.jenkinsci.plugins.workflow.job.WorkflowJob":
            "Pipeline (Scripted/Declarative)",
        "org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject":
            "Multibranch Pipeline",
        "project":                      "Freestyle",
        "matrix-project":               "Matrix",
        "com.tikal.jenkins.plugins.multijob.MultiJobProject": "MultiJob",
    }
    rec.pipeline_type = type_map.get(tag, tag)

    # ── Git remote URL ───────────────────────────────────────────────────────
    # Order matters — try the most specific paths first
    git_url = _xml_text(
        root,
        # Pipeline / Freestyle with Git plugin
        ".//scm/userRemoteConfigs/hudson.plugins.git.UserRemoteConfig/url",
        # Multibranch with git source
        ".//sources/data/jenkins.branch.BranchSource/source/remote",
        # Multibranch with GitHub source (repo owner + repo name handled below)
        ".//scm/remote",
    )

    # GitHub / Bitbucket SCM Source (Multibranch)
    gh_owner  = _xml_text(root,
        ".//sources/data/jenkins.branch.BranchSource/source/repoOwner")
    gh_repo   = _xml_text(root,
        ".//sources/data/jenkins.branch.BranchSource/source/repository")
    gh_server = _xml_text(root,
        ".//sources/data/jenkins.branch.BranchSource/source/serverUrl",
        ".//sources/data/jenkins.branch.BranchSource/source/apiUri")

    if git_url:
        rec.jenkinsfile_repo = git_url
    elif gh_owner and gh_repo:
        server = gh_server.rstrip("/") if gh_server else "https://github.com"
        rec.jenkinsfile_repo = f"{server}/{gh_owner}/{gh_repo}"

    # ── Branch ───────────────────────────────────────────────────────────────
    # IMPORTANT: do NOT use credentialsId paths — they look like branch nodes
    # but contain credential IDs, not branch names.
    raw_branch = _xml_text(
        root,
        # Git plugin — standard branch spec
        ".//scm/branches/hudson.plugins.git.BranchSpec/name",
        # Multibranch SCM source "includes" filter (e.g. "main develop")
        ".//sources/data/jenkins.branch.BranchSource/source/includes",
        # Pipeline job stored branch
        ".//definition/scm/branches/hudson.plugins.git.BranchSpec/name",
    )
    rec.jenkinsfile_branch = _clean_branch(raw_branch) if raw_branch else ""

    # ── Jenkinsfile path ─────────────────────────────────────────────────────
    rec.jenkinsfile_path = _xml_text(
        root,
        ".//definition/scriptPath",
        ".//scriptPath",
        ".//factory/scriptPath",
    ) or "Jenkinsfile"

    # ── Shared Libraries from config.xml (global/folder-level declarations) ─
    # These are job-level library declarations; @Library() usage in scripts
    # is more reliably captured from console output.
    libs_from_config = []
    for lib_el in root.findall(
        ".//libraries/org.jenkinsci.plugins.workflow.libs.LibraryConfiguration"
    ):
        lib_name = _xml_text(lib_el, "name")
        if lib_name:
            libs_from_config.append(lib_name)
    # Store as a hint — console parser will merge/override
    rec.shared_libraries = "; ".join(sorted(set(libs_from_config)))

    rec.source = "config.xml"
    return rec


# ---------------------------------------------------------------------------
# Tech-stack heuristics
# ---------------------------------------------------------------------------
TECH_KEYWORDS: dict[str, list[str]] = {
    "Maven":      ["mvn ", "maven", "pom.xml", "[INFO] BUILD"],
    "Gradle":     ["gradle", "gradlew", "BUILD SUCCESSFUL"],
    "npm":        ["npm install", "npm run", "npm ci", "yarn ", "package.json"],
    "Python":     ["pip install", "pip3 ", "python3 ", "pytest", "tox", "pipenv"],
    "Docker":     ["docker build", "docker push", "docker pull", "Dockerfile"],
    "Kubernetes": ["kubectl", "helm install", "helm upgrade", "k8s", "kubernetes"],
    "Terraform":  ["terraform init", "terraform plan", "terraform apply"],
    "Ansible":    ["ansible-playbook", "ansible "],
    ".NET":       ["dotnet build", "dotnet test", "msbuild", "nuget restore"],
    "Java":       ["javac ", "java -jar", "jdk", "jre"],
    "Go":         ["go build", "go test", "go mod"],
    "Ruby":       ["bundle exec", "gem install", "rake "],
    "Scala":      ["sbt ", "sbt compile"],
    "Shell":      ["#!/bin/bash", "#!/bin/sh", "sh '", 'sh "'],
}


def detect_tech_stack_from_text(text: str) -> str:
    """Detect tech stack by scanning raw text (console output or script)."""
    lower = text.lower()
    found = [tech for tech, kws in TECH_KEYWORDS.items()
             if any(kw.lower() in lower for kw in kws)]
    return "; ".join(found) if found else "Unknown"


# ---------------------------------------------------------------------------
# Console output parser — ALWAYS called; primary source for tech & libraries
# ---------------------------------------------------------------------------
def parse_console_output(console_text: str, rec: PipelineRecord) -> PipelineRecord:
    """
    Extract / enrich fields from the last build console output.

    - shared_libraries : ALWAYS populated from console (most reliable source)
    - tech_stack       : ALWAYS populated from console (most reliable source)
    - jenkinsfile_repo : filled if still missing
    - jenkinsfile_branch: filled if still missing
    """
    # ── Shared Libraries — primary source ────────────────────────────────────
    # Pattern 1: "Loading library my-lib@1.2.3"
    libs_loaded = re.findall(r"Loading library\s+([^\s@]+)@[\w.\-]+", console_text, re.I)
    # Pattern 2: "@Library('my-lib') _" in printed Jenkinsfile fragments
    libs_annotation = re.findall(r"@Library\(['\"]([^'\"]+)['\"]\)", console_text)
    # Pattern 3: "Library my-lib loaded"
    libs_loaded2 = re.findall(r"Library\s+([^\s]+)\s+loaded", console_text, re.I)

    all_libs = sorted(set(libs_loaded + libs_annotation + libs_loaded2))
    # Merge with anything already found from config.xml
    existing = [l for l in rec.shared_libraries.split("; ") if l]
    merged_libs = sorted(set(existing + all_libs))
    rec.shared_libraries = "; ".join(merged_libs) if merged_libs else ""

    # ── Tech Stack — primary source ──────────────────────────────────────────
    rec.tech_stack = detect_tech_stack_from_text(console_text)

    # ── Git repo (fallback if config.xml had none) ───────────────────────────
    if not rec.jenkinsfile_repo:
        m = re.search(
            r"(?:Cloning|Fetching|fetch|clone)\s+(?:the\s+)?(?:repository\s+)?['\"]?(https?://[^\s'\"]+|git@[^\s'\"]+)",
            console_text, re.I,
        )
        if m:
            rec.jenkinsfile_repo = m.group(1).rstrip("/")

    # ── Branch (fallback if config.xml had none or was wrong) ────────────────
    if not rec.jenkinsfile_branch:
        # "Checking out Revision abc123 (refs/remotes/origin/main)"
        m = re.search(r"Checking out Revision\s+\S+\s+\(([^)]+)\)", console_text)
        if m:
            rec.jenkinsfile_branch = _clean_branch(m.group(1).split(",")[0].strip())

    if not rec.jenkinsfile_branch:
        # "Branch: refs/heads/main"  or  "Branch: main"
        m = re.search(r"Branch(?:Name)?:\s*([\w/.\-]+)", console_text, re.I)
        if m:
            rec.jenkinsfile_branch = _clean_branch(m.group(1))

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
    1. Fetch config.xml        → parse SCM metadata, pipeline type, path
    2. Fetch console output    → ALWAYS; authoritative for tech_stack & shared_libraries,
                                 fallback for repo/branch if config.xml was empty
    3. Fetch last-run date
    """
    session = make_session()   # thread-local session
    rec = PipelineRecord(pipeline_url=job_url)

    # Derive display name from URL (last /job/<name> segment)
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

    # ── Step 2: Console output — ALWAYS fetched ─────────────────────────────
    # tech_stack and shared_libraries come from here (more reliable than XML).
    # Also fills in repo/branch when config.xml didn't have them.
    console_text = get_text(session, f"{job_url}/lastBuild/consoleText")
    if console_text:
        rec = parse_console_output(console_text, rec)
    else:
        logger.debug("No console output for %s (may never have run)", job_url)
        # If no console, try to get tech stack from config.xml inline script
        if not rec.tech_stack or rec.tech_stack == "Unknown":
            if config_text:
                try:
                    root = ET.fromstring(config_text)
                    inline = _xml_text(root, ".//definition/script", ".//script")
                    if inline:
                        rec.tech_stack = detect_tech_stack_from_text(inline)
                        # Also pick up @Library from inline script
                        inline_libs = re.findall(r"@Library\(['\"]([^'\"]+)['\"]\)", inline)
                        if inline_libs:
                            existing = [l for l in rec.shared_libraries.split("; ") if l]
                            rec.shared_libraries = "; ".join(sorted(set(existing + inline_libs)))
                except ET.ParseError:
                    pass

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
def _check_connectivity(session: requests.Session) -> None:
    """
    Fast connectivity pre-check before starting the full crawl.
    Exits with a clear error message if Jenkins is unreachable,
    rather than retrying 100k times and timing out.
    """
    probe_url = f"{JENKINS_URL}/api/json?tree=nodeName"
    logger.info("Connectivity check → %s (connect timeout: %ds)", probe_url, CONNECT_TIMEOUT)
    try:
        resp = session.get(probe_url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 401:
            logger.error(
                "Authentication failed (HTTP 401). "
                "Verify JENKINS_USER and JENKINS_TOKEN secrets are correct."
            )
            raise SystemExit(1)
        if resp.status_code == 403:
            logger.error(
                "Authorization failed (HTTP 403). "
                "The user lacks read permission on Jenkins."
            )
            raise SystemExit(1)
        if resp.status_code not in (200, 404):
            logger.error("Unexpected HTTP %s from Jenkins.", resp.status_code)
            raise SystemExit(1)
        logger.info("Jenkins is reachable ✓")
    except requests.exceptions.ConnectTimeout:
        logger.error(
            "\n"
            "═══════════════════════════════════════════════════════════\n"
            " CONNECTION TIMEOUT — cannot reach %s\n"
            "═══════════════════════════════════════════════════════════\n"
            " Likely causes:\n"
            "   1. Jenkins is on a private/corporate network.\n"
            "      → Use a self-hosted GitHub Actions runner that has\n"
            "        network access to Jenkins (recommended).\n"
            "   2. A firewall is blocking the runner's IP.\n"
            "      → Whitelist GitHub Actions IP ranges, or use a VPN\n"
            "        step (e.g. Tailscale) in the workflow.\n"
            "   3. Wrong JENKINS_URL — check the secret value.\n"
            "   4. Jenkins port is not 443 — include the port in the URL,\n"
            "      e.g. https://jenkins.example.com:8443\n"
            "   5. Self-signed certificate — set VERIFY_SSL=false secret.\n"
            "═══════════════════════════════════════════════════════════",
            JENKINS_URL,
        )
        raise SystemExit(1)
    except requests.exceptions.SSLError as exc:
        logger.error(
            "SSL certificate error: %s\n"
            "If Jenkins uses a self-signed cert, set the secret VERIFY_SSL=false",
            exc,
        )
        raise SystemExit(1)
    except requests.exceptions.ConnectionError as exc:
        logger.error("Connection error: %s", exc)
        raise SystemExit(1)


def main() -> None:
    logger.info("Jenkins Inventory starting …")
    logger.info("Target Jenkins:  %s", JENKINS_URL)
    logger.info("Workers: %d  |  Batch size: %d", MAX_WORKERS, BATCH_SIZE)
    logger.info("Timeouts: connect=%ds  read=%ds  |  SSL verify: %s",
                CONNECT_TIMEOUT, READ_TIMEOUT, VERIFY_SSL)

    if not JENKINS_URL or JENKINS_URL == "https://jenkins.example.com":
        logger.error("JENKINS_URL is not set. Export the environment variable and retry.")
        raise SystemExit(1)

    session = make_session()

    # Fail fast with a clear message before crawling 100k jobs
    _check_connectivity(session)

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
