# foundit_scraper.py
"""
Scraper for foundit.in searchResultsPage JSON endpoint.

Usage:
    from foundit_scraper import scrape
    df, offsets = scrape(
        search_term="accountant",
        location="kochi",
        results_wanted=20,
        offset=0,
        resume_offsets=None
    )
"""

import time
import random
import math
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urlparse
from datetime import datetime, timezone
import requests
import pandas as pd

# endpoint seen in your example
BASE_SEARCH_URL = "https://www.foundit.in/home/api/searchResultsPage"

# header pools to mimic legit devices
USER_AGENT_POOL = [
    # a few modern desktop UA strings
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
]

ACCEPT_LANGUAGE_POOL = ["en-US,en;q=0.9", "en-GB,en;q=0.9,en;q=0.8"]

DEFAULT_TIMEOUT = 15
POLITE_SLEEP = 0.15
DEFAULT_LIMIT = 20

# canonical column order used by scrapers.scraping_jobs
COLUMNS = [
    "title", "description", "job_url", "job_url_direct", "company",
    "company_url", "date_posted", "company_description", "location",
    "job_function", "source"
]


def _random_headers(referer: Optional[str] = None) -> dict:
    ua = random.choice(USER_AGENT_POOL)
    al = random.choice(ACCEPT_LANGUAGE_POOL)
    headers = {
        "User-Agent": ua,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": al,
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer or "https://www.foundit.in/",
        "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Connection": "keep-alive"
    }
    return headers


def _safe_get_json(session: requests.Session, url: str, params: dict = None, headers: dict = None, timeout: int = DEFAULT_TIMEOUT) -> Optional[dict]:
    attempt = 0
    while attempt < 3:
        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            try:
                return resp.json()
            except Exception:
                return None
        except Exception:
            attempt += 1
            time.sleep(1 + attempt * 0.5)
    return None


def _domain_from_url(u: str) -> str:
    try:
        if not u:
            return ""
        host = urlparse(u).hostname or ""
        parts = host.split(".")
        return parts[-2] if len(parts) >= 2 else host
    except Exception:
        return ""


def _format_location(loc_list: List[dict]) -> str:
    if not loc_list:
        return ""
    out = []
    for l in loc_list:
        city = l.get("city") or ""
        state = l.get("state") or ""
        country = l.get("country") or ""
        # sometimes 'city' may contain many names like "Cochin / Kochi / Ernakulam"
        parts = [p for p in (city.strip(), state.strip(), country.strip()) if p]
        out.append(", ".join(parts))
    # join multiple possible locations with " | "
    return " | ".join(out)


def _to_ddmmyyyy_from_ms(ms: Optional[int]) -> str:
    try:
        if not ms:
            return ""
        # many fields in sample are milliseconds since epoch
        ts = int(ms) / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return ""


def _normalize_foundit_item(item: dict) -> dict:
    # fields the user asked for: cleanedJobTitle, company name, company location (city,state,country or remote),
    # minimum/maximum experience and salary, posted date (DD/MM/YYYY), description, employment type,
    # skills, recruiterContactNumber, recruiterName.

    title = item.get("cleanedJobTitle") or item.get("title") or ""
    description = item.get("description") or ""
    # Construct job_url if site gives an explicit link later -- fallback to job detail path using jobId
    job_id = item.get("jobId") or item.get("id") or ""
    job_url = ""
    job_url_direct = ""
    if job_id:
        job_url = f"https://www.foundit.in/job/{job_id}"
        job_url_direct = job_url

    company = ""
    company_url = ""
    company_block = item.get("company") or {}
    if isinstance(company_block, dict):
        company = company_block.get("name") or item.get("companyName") or ""
        # foundit may not publish a company url in the item; leave blank if not present
        company_url = company_block.get("url") or company_block.get("website") or ""

    # date fields (ms -> DD/MM/YYYY)
    date_posted = _to_ddmmyyyy_from_ms(item.get("postedAt") or item.get("createdAt") or item.get("updatedAt"))

    # company description/profile
    company_description = item.get("companyProfile") or item.get("companyDescription") or ""

    # location: join city, state, country; items can have multiple location entries
    location = _format_location(item.get("locations") or [])

    # job function / roles
    job_function = ""
    if item.get("functions"):
        job_function = ", ".join(item.get("functions"))
    elif item.get("roles"):
        job_function = ", ".join(item.get("roles"))

    # source (domain) - use job_url_direct if available else infer from data (no explicit link in sample)
    source = _domain_from_url(job_url_direct) or item.get("jobSource") or ""

    # Additional fields you wanted extracted (we place these inside description or keep as extras if needed)
    # We'll append a short meta block to description with skills / exp / salary / recruiter so caller can extract if they want.
    skills_list = []
    for s in (item.get("skills") or []) + (item.get("itSkills") or []):
        if isinstance(s, dict):
            txt = s.get("text") or s.get("name") or ""
        else:
            txt = str(s)
        if txt:
            skills_list.append(txt)
    skills_str = ", ".join(dict.fromkeys([s.strip() for s in skills_list if s]))  # unique preserve order

    min_exp = ""
    max_exp = ""
    if item.get("minimumExperience") and isinstance(item["minimumExperience"], dict):
        try:
            min_exp = str(item["minimumExperience"].get("years", "")) + " yrs" if item["minimumExperience"].get("years") is not None else ""
        except Exception:
            min_exp = ""
    if item.get("maximumExperience") and isinstance(item["maximumExperience"], dict):
        try:
            max_exp = str(item["maximumExperience"].get("years", "")) + " yrs" if item["maximumExperience"].get("years") is not None else ""
        except Exception:
            max_exp = ""

    min_salary = ""
    max_salary = ""
    if item.get("minimumSalary") and isinstance(item["minimumSalary"], dict):
        v = item["minimumSalary"].get("absoluteValue")
        if v is not None:
            min_salary = f"{item['minimumSalary'].get('currency','') } {v}"
    if item.get("maximumSalary") and isinstance(item["maximumSalary"], dict):
        v = item["maximumSalary"].get("absoluteValue")
        if v is not None:
            max_salary = f"{item['maximumSalary'].get('currency','') } {v}"

    employment_type = ""
    if item.get("employmentTypes"):
        employment_type = ", ".join(item.get("employmentTypes"))

    recruiter_contact = item.get("recruiterContactNumber") or ""
    recruiter_name = item.get("recruiterName") or ""

    # Build a compact "description" that keeps original HTML plus an appended metadata block (plain text)
    meta_lines = []
    if skills_str:
        meta_lines.append(f"Skills: {skills_str}")
    if min_exp or max_exp:
        meta_lines.append(f"Experience: {min_exp or '?'} - {max_exp or '?'}")
    if min_salary or max_salary:
        meta_lines.append(f"Salary: {min_salary or '?'} - {max_salary or '?'}")
    if employment_type:
        meta_lines.append(f"EmploymentType: {employment_type}")
    if recruiter_name:
        meta_lines.append(f"Recruiter: {recruiter_name}")
    if recruiter_contact:
        meta_lines.append(f"RecruiterContact: {recruiter_contact}")

    if meta_lines:
        meta_block = "<!--META-->\n" + " | ".join(meta_lines) + "\n<!--/META-->\n"
        # keep original description (HTML) then meta block
        description = (description or "") + "\n\n" + meta_block

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


def scrape(
    search_term: str,
    location: str,
    results_wanted: int,
    offset: int = 0,
    resume_offsets: Optional[Dict[str, int]] = None,
    limit_per_request: int = DEFAULT_LIMIT,
    session: Optional[requests.Session] = None,
    allow_delay: bool = True,
    verbose: bool = False
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Perform a foundit search and return (DataFrame, offsets).
    - results_wanted: number of final rows to return (will be trimmed).
    - offset: 'start' parameter (0-based).
    - resume_offsets: dict with possible 'global' key to continue from persisted offset.
    - returned offsets: {"global": next_start}
    """
    wanted = int(results_wanted or 0)
    start = int(offset or 0)
    if isinstance(resume_offsets, dict) and resume_offsets.get("global") is not None:
        start = int(resume_offsets.get("global", start))

    s = session or requests.Session()
    headers = _random_headers(referer="https://www.foundit.in/")
    items: List[dict] = []
    fetched = 0
    overfetch_factor = 1.25
    target = int(math.ceil(max(1, wanted) * overfetch_factor)) if wanted > 0 else DEFAULT_LIMIT

    # foundit uses start & limit query params
    current_start = start
    while fetched < target:
        params = {
            "start": int(current_start),
            "limit": int(limit_per_request),
            "query": (search_term or ""),
            "locations": (location or ""),
            "queryDerived": "true",
            "countries": "India",
            "variantName": "DEFAULT",
        }
        if verbose:
            print("foundit: requesting", current_start, "limit", limit_per_request)
        headers = _random_headers(referer=f"https://www.foundit.in/search?query={search_term}&locations={location}")
        js = _safe_get_json(s, BASE_SEARCH_URL, params=params, headers=headers)
        if not js:
            # on error, break and return what we have
            break

        data = js.get("data") or js.get("results") or []
        if not isinstance(data, list) or not data:
            break

        for rec in data:
            try:
                normalized = _normalize_foundit_item(rec)
                items.append(normalized)
                fetched += 1
                if fetched >= target:
                    break
            except Exception:
                continue

        # advance start by number of returned items (safe for pagination)
        returned_count = len(data)
        if returned_count <= 0:
            break
        current_start += returned_count

        if allow_delay:
            time.sleep(POLITE_SLEEP)

    # Build DataFrame in canonical column order
    df = pd.DataFrame(items, columns=COLUMNS)
    # trim to the exact requested rows
    if wanted and df.shape[0] > wanted:
        df = df.iloc[:wanted].reset_index(drop=True)

    returned_offsets = {"global": int(current_start)}
    return df, returned_offsets


# quick local test when run as script
if __name__ == "__main__":
    # basic smoke test (will actually do network call)
    try:
        df, offs = scrape("accountant", "kochi", results_wanted=10, offset=0, verbose=True)
        print("rows:", df.shape[0], "offsets:", offs)
        print(df.head(3).to_dict(orient="records"))
    except Exception as e:
        print("error:", e)
