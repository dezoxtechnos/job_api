# scrapers.py
"""
Entry point used by tasks.py. Uses API scrapers for naukrigulf and gulftalent,
and falls back to jobspy scrape for other platforms (indeed/linkedin/google).
This version:
 - Accepts resume_offsets from caller (so internal/external offsets persisted in DB are used).
 - Returns nested offsets for naukri/gulftalent (internal/external), and integer offsets for others.
 - Exposes supports_platform(platform) helper so tasks can avoid creating jobworks for unsupported platforms.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from typing import List, Dict, Any, Optional, Tuple
import math
import pandas as pd

# imports (make sure these modules are in PYTHONPATH)
try:
    from naukrigulf_scraper import NaukriGulfScraper
except Exception:
    NaukriGulfScraper = None

try:
    from foundit_scraper import scrape as foundit_scrape
except Exception:
    foundit_scrape = None

try:
    from gulftalent_scraper import collect_internal_and_external
except Exception:
    collect_internal_and_external = None

# jobspy may not be importable in some environments
try:
    from jobspy import scrape_jobs as jobspy_scrape_jobs
    print("FAIL")
except Exception:
    jobspy_scrape_jobs = None
    print("FAIL")

# Column order expected by tasks.py
COLUMNS = [
    "title", "description", "job_url", "job_url_direct", "company",
    "company_url", "date_posted", "company_description", "location",
    "job_function", "source"
]


def supports_platform(platform: str) -> bool:
    """
    Return True if scraping for the platform is available in this environment.
    Platforms we know about: 'naukri', 'gulftalent', 'indeed', 'linkedin', 'google'
    """
    p = (platform or "").lower()
    if "naukri" in p:
        return NaukriGulfScraper is not None
    if "gulftalent" in p:
        return collect_internal_and_external is not None
    if p in ("indeed", "linkedin", "google"):
        return jobspy_scrape_jobs is not None
    # unknown platform -> not supported
    return False


def get_source_from_url(apply_url: str) -> str:
    try:
        hostname = urlparse(apply_url or "").hostname or ""
        parts = hostname.split(".")
        return parts[-2] if len(parts) >= 2 else (parts[0] if parts else "")
    except Exception:
        return ""


def _split_internal_external(total: int) -> Tuple[int, int]:
    total = max(0, int(total or 0))
    internal = (total + 1) // 2
    external = total - internal
    return internal, external


def compute_fetch_count_jobspy(requested: int) -> int:
    req = int(max(0, requested))
    if req <= 2:
        return req * 2
    if req <= 10:
        return req * 2
    if req <= 25:
        return int(math.ceil(req * 1.4))
    if req <= 50:
        return int(math.ceil(req * 1.3))
    return int(math.ceil(req * 1.5))


def normalize_naukri_api_item(item: dict) -> dict:
    if not isinstance(item, dict):
        return {c: "" for c in COLUMNS}
    title = item.get("company_name") or ""
    description = item.get("job_description_html") or ""
    job_url = item.get("internal_apply_url") or item.get("apply_url") or ""
    job_url_direct = item.get("external_apply_url") or item.get("apply_url") or job_url
    company = item.get("company_name") or ""
    company_url = item.get("company_url") or ""
    date_posted = item.get("posted_date") or ""
    company_description = item.get("company_about") or ""
    location = item.get("location") or ""
    job_function = item.get("job_function") or ""
    source = item.get("source") or get_source_from_url(job_url_direct)
    return {
        "title": title,
        "description": description,
        "job_url": job_url,
        "job_url_direct": job_url_direct,
        "company": company,
        "company_url": company_url,
        "date_posted": date_posted,
        "company_description": company_description,
        "location": location,
        "job_function": job_function,
        "source": source
    }


def normalize_gulftalent_parsed(parsed_wrapper: dict) -> dict:
    if not isinstance(parsed_wrapper, dict):
        return {c: "" for c in COLUMNS}
    meta = parsed_wrapper.get("job_meta", {}) or {}
    parsed = parsed_wrapper.get("parsed", {}) or {}
    title = meta.get("title") or meta.get("job_title") or meta.get("positionTitle") or ""
    description = parsed.get("job_description_html") or parsed.get("external_page_html_snippet") or ""
    job_url = parsed.get("job_url") or (meta.get("link") and ("https://www.gulftalent.com" + meta.get("link"))) or ""
    job_url_direct = parsed.get("apply_url") or job_url
    company = meta.get("company_name") or meta.get("company") or ""
    company_url = parsed.get("company_url") or ""
    date_posted = meta.get("posted_date_ts") or ""
    company_description = parsed.get("company_description_html") or ""
    location = meta.get("location") or meta.get("city") or ""
    job_function = meta.get("job_function") or ""
    source = parsed.get("source") or get_source_from_url(job_url_direct)
    return {
        "title": title,
        "description": description,
        "job_url": job_url,
        "job_url_direct": job_url_direct,
        "company": company,
        "company_url": company_url,
        "date_posted": date_posted,
        "company_description": company_description,
        "location": location,
        "job_function": job_function,
        "source": source
    }


def scraping_jobs(site_name,
                  search_term,
                  google_search_term,
                  location,
                  job_type,
                  results_wanted,
                  offset,
                  country_indeed,
                  description_format="html",
                  search_id=None,
                  user_token=None,
                  resume_offsets: Optional[Dict[str, int]] = None):
    try:
        results_wanted = int(results_wanted or 0)
    except Exception:
        results_wanted = 0
    try:
        offset = int(offset or 0)
    except Exception:
        offset = 0

    all_jobs: List[dict] = []
    site_names = [s.lower() for s in (site_name or [])]

    # NAUKRI (API)
    if any("naukri" in s for s in site_names) and NaukriGulfScraper is not None:
        internal_count, external_count = _split_internal_external(results_wanted)
        ng = NaukriGulfScraper()
        resume = None
        if isinstance(resume_offsets, dict):
            resume = {"easy": int(resume_offsets.get("internal", offset)), "external": int(resume_offsets.get("external", offset))}
        else:
            resume = {"easy": offset, "external": offset}
        out = ng.scrape(
            search_term=search_term,
            location=location,
            total_easy=internal_count,
            total_external=external_count,
            resume_offsets=resume
        )
        easy = out.get("easy_jobs") or []
        external = out.get("external_jobs") or []
        for item in easy:
            all_jobs.append(normalize_naukri_api_item(item))
        for item in external:
            all_jobs.append(normalize_naukri_api_item(item))
        df = pd.DataFrame(all_jobs, columns=COLUMNS)
        offsets = out.get("offsets") or {}
        internal_off = int(offsets.get("easy", 0))
        external_off = int(offsets.get("external", 0))
        if internal_off > 0 and external_off > 0:
            global_off = min(internal_off, external_off)
        else:
            global_off = internal_off or external_off or int(offset)
        returned_offsets = {"internal": internal_off, "external": external_off, "global": int(global_off)}
        return df, returned_offsets

    # GULFTALENT (API)
    if any("gulftalent" in s for s in site_names) and collect_internal_and_external is not None:
        internal_count, external_count = _split_internal_external(results_wanted)
        start_offset = int(offset)
        if isinstance(resume_offsets, dict) and "internal" in resume_offsets:
            start_offset = int(resume_offsets.get("internal", offset))
        res = collect_internal_and_external(
            n_internal=internal_count,
            n_external=external_count,
            keyword=search_term,
            country=country_indeed or "",
            job_type=job_type or "",
            location=location or "",
            page_limit=25,
            session=None,
            allow_delay=True,
            verbose=False,
            start_offset=start_offset
        )
        internal = res.get("internal_parsed") or []
        external = res.get("external_parsed") or []
        for p in internal:
            all_jobs.append(normalize_gulftalent_parsed(p))
        for p in external:
            all_jobs.append(normalize_gulftalent_parsed(p))
        df = pd.DataFrame(all_jobs, columns=COLUMNS)
        internal_off = int(res.get("internal_offset", 0))
        external_off = int(res.get("external_offset", 0))
        if internal_off > 0 and external_off > 0:
            global_off = min(internal_off, external_off)
        else:
            global_off = internal_off or external_off or int(offset)
        returned_offsets = {"internal": internal_off, "external": external_off, "global": int(global_off)}
        return df, returned_offsets


    # if any("foundit" in s for s in site_names) and foundit_scrape is not None:
    #     start_offset = int(offset)
    #     if isinstance(resume_offsets, dict) and resume_offsets.get("global") is not None:
    #         start_offset = int(resume_offsets.get("global", start_offset))
    #     df, returned_offsets = foundit_scrape(
    #         search_term=search_term,
    #         location=location,
    #         results_wanted=results_wanted,
    #         offset=start_offset,
    #         resume_offsets=resume_offsets
    #     )
    #     if df is None:
    #         return pd.DataFrame([], columns=COLUMNS)
    #     return df, returned_offsets
    
    # jobspy for indeed/linkedin/google
    for platform in ("indeed", "linkedin", "google"):
        if platform in site_names:
            if jobspy_scrape_jobs is None:
                return None
            fetch_count = compute_fetch_count_jobspy(results_wanted)
            df = jobspy_scrape_jobs(
                site_name=[platform],
                search_term=search_term,
                google_search_term=google_search_term,
                location=location,
                job_type='fulltime',
                results_wanted=fetch_count,
                offset=offset,
                country_indeed=country_indeed,
                description_format=description_format,
                linkedin_fetch_description=(platform == "linkedin"),
                hours_old=(120 if platform == "linkedin" else None)
            )
            if df is None:
                return pd.DataFrame([], columns=COLUMNS)
            if hasattr(df, "shape") and df.shape[0] > results_wanted:
                df = df.iloc[:results_wanted].reset_index(drop=True)
            return df

    return pd.DataFrame([], columns=COLUMNS)
