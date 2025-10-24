# gulftalent_scraper.py
import json
import random
import time
import re
from html import escape
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
from urllib.parse import urljoin
from typing import Tuple
from bs4 import Tag

GULF_BASE = "https://www.gulftalent.com"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
]

BASE_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "DNT": "1",
    "X-Requested-With": "XMLHttpRequest",
    "Upgrade-Insecure-Requests": "1",
}

COUNTRY_MAP = {
    "united arab emirates": "10111111000000",
    "saudi arabia": "10111112000000",
    "qatar": "10111114000000",
    "kuwait": "10111113000000",
    "oman": "10111116000000",
}

LOCATION_MAP = {
    "abu dhabi": "10111111000112",
    "ajman": "10111111000114",
    "dubai": "10111111000111",
    "fujairah": "10111111000116",
    "ras al khaimah": "10111111000117",
    "sharjah": "10111111000113",
    "dammam": "10111112000123",
    "jeddah": "10111112000122",
    "riyadh": "10111112000121",
    "muscat": "10111116000162",
    "doha": "10111114000151",
    "kuwait city": "10111113000131",
}

EMPLOYMENT_MAP = {
    "full-time": "1",
    "part-time": "2",
    "contract": "5",
    "remote": ""
}

def normalize_timestamp(ts) -> Optional[str]:
    try:
        ts = int(ts)
        if ts > 10**12:
            ts = ts // 1000
        if ts <= 0:
            return None
        return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def looks_like_html(text: Optional[str]) -> bool:
    if not text or not isinstance(text, str):
        return False
    return bool(re.search(r'^\s*<(?:!doctype|html)', text, flags=re.I))

def text_to_html(raw_text: str) -> str:
    if not raw_text:
        return ""
    t = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.rstrip() for ln in t.split("\n")]
    html_parts = []
    in_ul = False
    buffer_para = []

    def flush_para():
        nonlocal buffer_para
        if buffer_para:
            txt = " ".join(buffer_para).strip()
            if txt:
                html_parts.append(f"<p>{escape(txt)}</p>")
            buffer_para = []

    for ln in lines:
        stripped = ln.strip()
        if not stripped:
            flush_para()
            if in_ul:
                html_parts.append("</ul>")
                in_ul = False
            continue

        if re.match(r'^[\u2022\-\*\•\t]+\s*(.+)', ln) or stripped.startswith("•") or stripped.startswith("-") or stripped.startswith("*"):
            flush_para()
            if not in_ul:
                html_parts.append("<ul>")
                in_ul = True
            bullet = re.sub(r'^[\u2022\-\*\•\t\s]+', '', ln)
            html_parts.append(f"<li>{escape(bullet.strip())}</li>")
            continue

        if re.match(r'^(Key Responsibilities|Responsibilities|Requirements|What We Offer|About the Client|Job Description)[:\s\-]*', stripped, flags=re.I):
            flush_para()
            if in_ul:
                html_parts.append("</ul>")
                in_ul = False
            html_parts.append(f"<h4>{escape(stripped)}</h4>")
            continue

        buffer_para.append(stripped)

    flush_para()
    if in_ul:
        html_parts.append("</ul>")

    return "\n".join(html_parts)

def parse_fulltext_position(fulltext: str, job_url: Optional[str] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "job_url": None,
        "apply_url": None,
        "has_external_application": 0,
        "job_description_html": "",
        "company_description_html": "",
        "company_text": ""
    }
    if not fulltext:
        return out
    try:
        if job_url:
            out["job_url"] = urljoin(GULF_BASE, job_url)
            out["apply_url"] = out["job_url"]
    except Exception:
        out["job_url"] = job_url

    parts = [p for p in re.split(r'\+{4,}', fulltext) if p and p.strip()]
    if parts and re.match(r'^[A-Z0-9\-]{3,}$', parts[0].strip()):
        parts = parts[1:]

    title = parts[0].strip() if len(parts) >= 1 else None
    desc = parts[1].strip() if len(parts) >= 2 else ""
    reqs = parts[2].strip() if len(parts) >= 3 else ""

    company_parts = []
    if len(parts) == 4:
        company_parts = [parts[3]]
    elif len(parts) == 5:
        company_parts = [parts[4]]
    elif len(parts) > 5:
        company_parts = parts[4:]

    job_html_parts = []
    if title:
        job_html_parts.append(f"<div class='gt-job'><h3>{escape(title)}</h3>")
    else:
        job_html_parts.append("<div class='gt-job'>")

    if desc:
        job_html_parts.append("<h4>Job description</h4>")
        job_html_parts.append(text_to_html(desc))

    if reqs:
        job_html_parts.append("<h4>Requirements</h4>")
        job_html_parts.append(text_to_html(reqs))

    job_html_parts.append("</div>")
    out["job_description_html"] = "\n".join(job_html_parts)

    company_text = " ".join([p.strip() for p in company_parts if p and p.strip()]) if company_parts else ""
    out["company_text"] = re.sub(r'\s+', ' ', company_text).strip()
    if out["company_text"]:
        out["company_description_html"] = f"<div class='gt-company'><h4>About the company</h4><p>{escape(out['company_text'])}</p></div>"

    return out

def clean_and_dedupe_html(html_fragment: str) -> str:
    """
    Remove 'Employment' blocks and dedupe repeated top-level blocks by normalized text.
    Keeps the first occurrence of each unique block and returns joined HTML.
    """
    if not html_fragment:
        return ""

    soup = BeautifulSoup(html_fragment, "html.parser")

    # Drop nodes that clearly are the employment block (conservative)
    for s in soup.find_all(string=lambda t: t and 'employment' in t.lower()):
        parent = getattr(s, "parent", None)
        # climb to a block-level container if possible
        top = parent
        while top is not None and getattr(top, "name", None) not in ("div", "section", "article", "p", "ul", "ol", "li"):
            top = getattr(top, "parent", None)
        try:
            if top:
                top.decompose()
            elif parent:
                parent.decompose()
        except Exception:
            try:
                s.extract()
            except Exception:
                pass

    # Build list of top-level blocks (use body children if present)
    top_children = list(soup.body.contents) if soup.body else list(soup.contents)
    seen = set()
    kept = []
    for child in top_children:
        # get normalized text
        if isinstance(child, Tag):
            txt = child.get_text(" ", strip=True)
        else:
            txt = str(child).strip()
        norm = re.sub(r'\s+', ' ', txt).strip()
        if not norm:
            continue
        if norm in seen:
            continue
        seen.add(norm)
        kept.append(child)

    # Return combined HTML of kept nodes
    return "".join(str(x) for x in kept)


def extract_job_and_company_from_panel(panel_or_soup) -> Tuple[str, str]:
    if panel_or_soup is None:
        return ("", "")

    def collect_after_header(header_tag):
        parts = []
        for elem in header_tag.find_all_next():
            if isinstance(elem, Tag) and elem.name and elem.name.lower() in ("h2", "h3", "h4", "h5"):
                if elem == header_tag:
                    continue
                break
            if isinstance(elem, Tag):
                parts.append(str(elem))
        return "\n".join(parts).strip()

    if isinstance(panel_or_soup, Tag) and panel_or_soup.name and panel_or_soup.name.lower() != "html":
        panel = panel_or_soup
        job_header = None
        for tagname in ("h3", "h4", "h2", "h5"):
            job_header = panel.find(tagname, string=lambda t: t and re.search(r'job\s*description|role|job\s*description\s*/\s*role|about\s+the\s+job', t, flags=re.I))
            if job_header:
                break
        if not job_header:
            job_header = panel.find(lambda t: getattr(t, "name", None) in ("h2","h3","h4","h5") and 'job' in t.get_text(strip=True).lower() and 'description' in t.get_text(strip=True).lower())

        job_html = ""
        if job_header:
            job_html = collect_after_header(job_header)
        else:
            job_html = "".join(str(child) for child in panel.contents).strip()

        comp_header = None
        for tagname in ("h4", "h3", "h2", "h5"):
            comp_header = panel.find(tagname, string=lambda t: t and re.search(r'about\s+the\s+company|about\s+company|about\s+the\s+client', t, flags=re.I))
            if comp_header:
                break

        company_text = ""
        if comp_header:
            parts = []
            for elem in comp_header.find_all_next():
                if isinstance(elem, Tag) and elem.name and elem.name.lower() in ("h2","h3","h4","h5") and elem != comp_header:
                    break
                if isinstance(elem, Tag):
                    parts.append(elem.get_text(" ", strip=True))
            company_text = " ".join([p for p in parts if p]).strip()

        return (job_html or "", company_text or "")

    soup = panel_or_soup
    job_header = None
    for tagname in ("h3", "h4", "h2", "h5"):
        job_header = soup.find(tagname, string=lambda t: t and re.search(r'job\s*description|role|job\s*description\s*/\s*role', t, flags=re.I))
        if job_header:
            break
    if not job_header:
        job_header = soup.find(lambda t: getattr(t, "name", None) in ("h2","h3","h4","h5") and 'job' in t.get_text(strip=True).lower() and ('description' in t.get_text(strip=True).lower() or 'role' in t.get_text(strip=True).lower()))

    job_html = ""
    if job_header:
        job_html = collect_after_header(job_header)

    comp_header = None
    for tagname in ("h4", "h3", "h2", "h5"):
        comp_header = soup.find(tagname, string=lambda t: t and re.search(r'about\s+the\s+company|about\s+company|about\s+the\s+client', t, flags=re.I))
        if comp_header:
            break

    company_text = ""
    if comp_header:
        parts = []
        for elem in comp_header.find_all_next():
            if isinstance(elem, Tag) and elem.name and elem.name.lower() in ("h2","h3","h4","h5") and elem != comp_header:
                break
            if isinstance(elem, Tag):
                parts.append(elem.get_text(" ", strip=True))
        company_text = " ".join([p for p in parts if p]).strip()

    if not job_html:
        panel = soup.select_one("div.panel-body.job-description") or soup.select_one("div.panel-body.content-visibility-auto.job-description")
        if panel:
            return extract_job_and_company_from_panel(panel)

    return (job_html or "", company_text or "")

def make_session(proxies: Optional[Dict[str, str]] = None, extra_headers: Optional[Dict[str, str]] = None) -> requests.Session:
    s = requests.Session()
    headers = BASE_HEADERS.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
    headers["Referer"] = GULF_BASE + "/"
    if extra_headers:
        headers.update(extra_headers)
    s.headers.update(headers)

    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "POST"])
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    if proxies:
        s.proxies.update(proxies)

    try:
        s.get(GULF_BASE + "/", timeout=8)
    except Exception:
        pass

    return s

def fetch_jobs_page(keyword: str = "", country: str = "", job_type: str = "", location: str = "", limit: int = 25, offset: int = 0, session: Optional[requests.Session] = None, allow_delay: bool = True, timeout: int = 15) -> Optional[List[Dict[str, Any]]]:
    country_key = (country or "").strip().lower()
    location_key = (location or "").strip().lower()
    job_type_key = (job_type or "").strip().lower()
    keyword = keyword or ""

    country_code = COUNTRY_MAP.get(country_key, "")
    location_code = LOCATION_MAP.get(location_key, "")
    employment_type = EMPLOYMENT_MAP.get(job_type_key, "")

    params = {
        "config[filters]": "DISABLED",
        "config[isDynamicSearchV2]": "true",
        "filters[city][0]": location_code,
        "filters[country][0]": country_code,
        "employment_type": employment_type,
        "filters[search_keyword]": keyword,
        "include_scraped": "1",
        "limit": str(limit),
        "offset": str(offset),
        "search_keyword": keyword,
        "search_order": "d",
        "version": "2",
    }

    if session is None:
        session = make_session()

    if allow_delay:
        time.sleep(random.uniform(0.15, 0.8))

    url = urljoin(GULF_BASE, "/api/jobs/search")
    try:
        r = session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        return data.get("results", {}).get("data", [])
    except Exception:
        return None

def _extract_from_jsonld(soup: BeautifulSoup) -> Dict[str, str]:
    out = {"job_description_html": "", "company_text": ""}
    if soup is None:
        return out
    scripts = soup.find_all("script", type=lambda t: t and "ld+json" in t)
    for s in scripts:
        try:
            txt = s.string or s.get_text()
            if not txt:
                continue
            parsed = json.loads(txt.strip())
            candidates = parsed if isinstance(parsed, list) else [parsed]
            def walk(node):
                if not node:
                    return None
                if isinstance(node, dict):
                    t = node.get("@type") or node.get("type")
                    if isinstance(t, list):
                        t = t[0]
                    if t and isinstance(t, str) and "JobPosting" in t:
                        return node
                    for v in node.values():
                        res = walk(v)
                        if res:
                            return res
                elif isinstance(node, list):
                    for item in node:
                        res = walk(item)
                        if res:
                            return res
                return None
            for cand in candidates:
                jp = walk(cand)
                if not jp:
                    continue
                desc = jp.get("description") or jp.get("jobDescription") or ""
                if isinstance(desc, str) and desc.strip():
                    out["job_description_html"] = desc.strip()
                ho = jp.get("hiringOrganization") or {}
                if isinstance(ho, dict):
                    name = ho.get("name") or ""
                else:
                    name = ho if isinstance(ho, str) else ""
                if name:
                    out["company_text"] = name.strip()
                if out["job_description_html"] or out["company_text"]:
                    return out
        except Exception:
            continue
    return out

def _extract_fallback_selectors(soup: BeautifulSoup) -> str:
    if soup is None:
        return ""
    selectors = [
        "div.panel-body.job-description",
        "div.panel-body.content-visibility-auto.job-description",
        "div.job-description",
        "div.description",
        "section.job-description",
        "section.job-desc",
        "#jobDescriptionText",
        ".job-desc",
        "article.job-posting",
        "div[itemprop='description']",
    ]
    for sel in selectors:
        node = soup.select_one(sel)
        if node:
            return "".join(str(c) for c in node.contents).strip()
    candidates = soup.find_all("div")
    best = ""
    for c in candidates:
        txt = c.get_text(" ", strip=True)[:5000]
        if len(txt) > len(best):
            best = txt
    return f"<p>{escape(best)}</p>" if best else ""

def parse_internal(job_link: str, session: Optional[requests.Session] = None, timeout: int = 12) -> Dict[str, Any]:
    session = session or make_session()
    job_url = urljoin(GULF_BASE + "/", job_link.lstrip("/"))
    try:
        r = session.get(job_url, timeout=timeout)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        panel = soup.select_one("div.panel-body.job-description") or soup.select_one("div.panel-body.content-visibility-auto.job-description")
        job_html, company_text = extract_job_and_company_from_panel(panel or soup)

        if not (job_html and job_html.strip()):
            fallback_html = _extract_fallback_selectors(soup)
            if fallback_html and fallback_html.strip():
                job_html = fallback_html

        if not (job_html and job_html.strip()):
            jl = _extract_from_jsonld(soup)
            if jl.get("job_description_html"):
                job_html = jl["job_description_html"]
            if jl.get("company_text") and not company_text:
                company_text = jl["company_text"]

        if not company_text:
            ho = soup.select_one("[itemprop='hiringOrganization'] [itemprop='name']")
            if ho:
                company_text = ho.get_text(" ", strip=True)
        if not company_text:
            cnode = soup.select_one(".company-name, .employer-name, .companyHeader, .company")
            if cnode:
                company_text = cnode.get_text(" ", strip=True)

        cleaned_job_html = clean_and_dedupe_html(job_html)
        cleaned_job_html = re.sub(r'(?is)employment[:\s-]*((full|part)\s*time)?', '', cleaned_job_html).strip()


        company_description_html = f"<div class='gt-company'><h4>About the company</h4><p>{escape(company_text)}</p></div>" if company_text else ""

        return {
            "job_url": job_url,
            "has_external_application": 0,
            "job_description_html": cleaned_job_html or "",
            "company_description_html": company_description_html,
            "apply_url": job_url,
            "source": "gulftalent",
        }
    except Exception as e:
        return {"error": f"internal_parse_failed: {str(e)}", "job_url": job_url}

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
              "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Cookie": ""
}

def find_external_apply_url(job_id: int, session: Optional[requests.Session] = None, timeout: int = 12) -> Dict[str, Any]:
    """
    Resolve /jobs/external?jobid={id} and return the destination apply_url only.
    IMPORTANT: do NOT return the /jobs/external URL as job_url (we must not store it).
    """
    session = session or make_session()
    external_url = f"{GULF_BASE}/jobs/external?jobid={job_id}"
    try:
        r = session.get(external_url, allow_redirects=False, timeout=timeout, headers=headers)
        # If there's a Location header, that's the redirect destination (could be external or GT canonical)
        if "Location" in r.headers:
            dest = urljoin(GULF_BASE, r.headers["Location"])
            return {"apply_url": dest, "has_external_application": 1}
        # Otherwise parse the page to find meta-refresh, anchor or form action
        soup = BeautifulSoup(r.text or "", "html.parser")
        meta = soup.find("meta", attrs={"http-equiv": lambda v: v and v.lower() == "refresh"})
        if meta and "content" in meta.attrs:
            try:
                _, urlpart = meta["content"].split(";", 1)
                _, urlval = urlpart.split("=", 1)
                apply = urlval.strip().strip("'\"")
                apply = urljoin(GULF_BASE, apply)
                return {"apply_url": apply, "has_external_application": 1}
            except Exception:
                pass
        a = soup.find("a", href=True)
        if a:
            return {"apply_url": urljoin(GULF_BASE, a["href"]), "has_external_application": 1}
        form = soup.find("form", action=True)
        if form:
            return {"apply_url": urljoin(GULF_BASE, form["action"]), "has_external_application": 1}
        # fallback: indicate we couldn't find an external destination beyond the external page itself.
        return {"apply_url": None, "has_external_application": 1, "note": "no-redirect-found-in-headers-or-html"}
    except Exception as e:
        return {"apply_url": None, "error": f"external_parse_failed: {str(e)}"}

def parse_job(job: Dict[str, Any], session_factory=None, timeout: int = 12, allow_delay: bool = True) -> Dict[str, Any]:
    session = session_factory() if session_factory else make_session()
    if allow_delay:
        time.sleep(random.uniform(0.05, 0.4))

    parsed: Dict[str, Any] = {}
    job_id = job.get("position_id") or job.get("id") or job.get("positionId") or None
    has_external = int(job.get("has_external_application", 0) or 0)

    try:
        ts_val = job.get("posted_date_ts") or job.get("posted_date") or None
        parsed["posted_date_iso"] = normalize_timestamp(ts_val) if ts_val is not None else None
    except Exception:
        parsed["posted_date_iso"] = None

    try:
        fulltext = job.get("fulltext_position") or job.get("fulltext") or None
        if looks_like_html(fulltext):
            fulltext = None

        link_val = job.get("link") or job.get("url") or None
        canonical_job_url = urljoin(GULF_BASE, link_val) if link_val else None

        # 1) If API fulltext is usable, prefer it
        if fulltext and isinstance(fulltext, str) and fulltext.strip():
            ft = parse_fulltext_position(fulltext, job_url=canonical_job_url)
            parsed.update(ft)

            # attach apply_url for external jobs (do not overwrite job_url)
            if has_external:
                if not job_id:
                    parsed.setdefault("notes", []).append("missing_job_id_for_external")
                    parsed["has_external_application"] = 1
                else:
                    ext = find_external_apply_url(int(job_id), session=session, timeout=timeout)
                    if ext.get("apply_url"):
                        parsed["apply_url"] = ext["apply_url"]
                    parsed["has_external_application"] = 1
            else:
                if canonical_job_url:
                    parsed["apply_url"] = canonical_job_url
                    parsed["job_url"] = canonical_job_url
                    parsed["has_external_application"] = 0
            return {"job_meta": job, "parsed": parsed}

        # 2) No usable fulltext -> parse canonical job page first (cover both internal and external)
        if link_val:
            parsed_internal = parse_internal(link_val, session=session, timeout=timeout)
            parsed.update(parsed_internal)
            parsed.setdefault("has_external_application", 0)
        else:
            parsed.setdefault("job_description_html", "")
            parsed.setdefault("company_description_html", "")
            parsed.setdefault("job_url", None)
            parsed.setdefault("has_external_application", has_external)

        # 3) If external, resolve apply_url but do NOT store /jobs/external as job_url
        if has_external:
            if not job_id:
                parsed.setdefault("notes", []).append("missing_job_id_for_external")
                parsed["has_external_application"] = 1
                return {"job_meta": job, "parsed": parsed}

            ext = find_external_apply_url(int(job_id), session=session, timeout=timeout)
            if ext.get("apply_url"):
                parsed["apply_url"] = ext["apply_url"]
            parsed["has_external_application"] = 1

            # If canonical link missing earlier and apply_url points to a GT canonical job page,
            # parse it and set job_url to that canonical page (rare).
            if (not link_val) and ext.get("apply_url") and "gulftalent.com" in ext.get("apply_url") and "/jobs/external" not in ext.get("apply_url"):
                try:
                    parsed_from_apply = parse_internal(ext.get("apply_url"), session=session, timeout=timeout)
                    if parsed_from_apply.get("job_description_html"):
                        parsed["job_description_html"] = parsed_from_apply["job_description_html"]
                    if parsed_from_apply.get("company_description_html"):
                        parsed["company_description_html"] = parsed_from_apply["company_description_html"]
                    parsed["job_description_html"] = clean_and_dedupe_html(parsed.get("job_description_html", "") or "")
                    parsed["job_description_html"] = re.sub(r'(?is)employment[:\s-]*((full|part)\s*time)?', '', parsed["job_description_html"]).strip()
                    parsed.setdefault("job_url", ext.get("apply_url"))
                except Exception:
                    pass
        else:
            parsed.setdefault("has_external_application", 0)
            if canonical_job_url:
                parsed.setdefault("apply_url", canonical_job_url)
                parsed.setdefault("job_url", canonical_job_url)

        return {"job_meta": job, "parsed": parsed}

    except Exception as e:
        parsed["error"] = f"parse_job_failed: {str(e)}"
        return {"job_meta": job, "parsed": parsed}

def fetch_jobs_all(keyword: str = "", country: str = "", job_type: str = "", location: str = "", max_jobs: Optional[int] = None, page_limit: int = 25, session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    session = session or make_session()
    offset = 0
    all_jobs: List[Dict[str, Any]] = []
    while True:
        batch = fetch_jobs_page(keyword=keyword, country=country, job_type=job_type, location=location, limit=page_limit, offset=offset, session=session)
        if not batch:
            break
        all_jobs.extend(batch)
        offset += page_limit
        if max_jobs and len(all_jobs) >= max_jobs:
            return all_jobs[:max_jobs]
        time.sleep(random.uniform(0.2, 0.9))
    return all_jobs

def parse_jobs_concurrent(jobs: List[Dict[str, Any]], workers: int = 8, apply_type: Optional[int] = None, session_factory=None, timeout: int = 12, allow_delay: bool = True) -> List[Dict[str, Any]]:
    if apply_type is not None:
        jobs = [j for j in jobs if int(j.get("has_external_application", 0) or 0) == int(apply_type)]

    results: List[Dict[str, Any]] = []
    session_factory = session_factory or (lambda: make_session())

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(parse_job, job, session_factory, timeout, allow_delay): job for job in jobs}
        for fut in as_completed(futures):
            try:
                res = fut.result()
            except Exception as e:
                res = {"error": f"worker_exception: {e}", "job_meta": futures[fut]}
            results.append(res)
    return results

def save_json(data: List[Dict[str, Any]], filename: str = "gulftalent_jobs.json") -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def collect_internal_and_external(
    n_internal: int,
    n_external: int,
    keyword: str = "",
    country: str = "",
    job_type: str = "",
    location: str = "",
    page_limit: int = 25,
    session=None,
    allow_delay: bool = True,
    verbose: bool = True,
    start_offset: int = 0,
) -> Dict[str, object]:
    if session is None:
        session = make_session()

    total_requested = max(0, int(n_internal)) + max(0, int(n_external))
    hard_cap = max(100, total_requested * 2)

    internal_parsed: List[Dict[str, Any]] = []
    external_parsed: List[Dict[str, Any]] = []

    last_internal_pos: Optional[int] = None
    last_external_pos: Optional[int] = None

    processed = 0
    offset = int(start_offset or 0)
    batches = 0

    while processed < hard_cap and (len(internal_parsed) < n_internal or len(external_parsed) < n_external):
        if allow_delay:
            time.sleep(random.uniform(0.15, 0.8))

        batch = fetch_jobs_page(keyword=keyword, country=country, job_type=job_type, location=location,
                                limit=page_limit, offset=offset, session=session)
        batches += 1

        if not batch:
            break

        for job in batch:
            processed += 1
            try:
                is_external = bool(int(job.get("has_external_application", 0) or 0))
            except Exception:
                is_external = False

            # INTERNAL
            if not is_external and len(internal_parsed) < n_internal:
                fulltext = job.get("fulltext_position") or job.get("fulltext")
                if looks_like_html(fulltext):
                    fulltext = None
                if fulltext and isinstance(fulltext, str) and fulltext.strip():
                    link_val = job.get("link") or job.get("url") or None
                    parsed = parse_fulltext_position(fulltext, job_url=(urljoin(GULF_BASE, link_val) if link_val else None))
                else:
                    link = job.get("link", "")
                    if link:
                        parsed = parse_internal(link, session=session, timeout=12)
                    else:
                        parsed = {"error": "missing_link_for_internal", "job_url": None}
                internal_parsed.append({"job_meta": job, "parsed": parsed})
                last_internal_pos = processed + offset

            # EXTERNAL: parse canonical job page first, then resolve apply_url (but do not store /jobs/external as job_url)
            if is_external and len(external_parsed) < n_external:
                fulltext = job.get("fulltext_position") or job.get("fulltext")
                if looks_like_html(fulltext):
                    fulltext = None
                link_val = job.get("link") or job.get("url") or None
                canonical_job_url = urljoin(GULF_BASE, link_val) if link_val else None

                parsed: Dict[str, Any] = {}

                if fulltext and isinstance(fulltext, str) and fulltext.strip():
                    parsed = parse_fulltext_position(fulltext, job_url=canonical_job_url)
                else:
                    if link_val:
                        parsed = parse_internal(link_val, session=session, timeout=12)
                    else:
                        parsed = {"job_description_html": "", "company_description_html": "", "job_url": None}

                job_id = job.get("position_id") or job.get("id") or None
                if job_id:
                    ext = find_external_apply_url(int(job_id), session=session, timeout=12)
                    # attach only apply_url and has_external_application; preserve parsed['job_url'] (canonical) if present
                    if ext.get("apply_url"):
                        parsed["apply_url"] = ext["apply_url"]
                    parsed["has_external_application"] = 1

                    # If canonical link missing but apply_url is a GT job page, parse that GT canonical apply_url to fill the description and job_url
                    if (not link_val) and ext.get("apply_url") and "gulftalent.com" in ext.get("apply_url") and "/jobs/external" not in ext.get("apply_url"):
                        try:
                            parsed_from_apply = parse_internal(ext.get("apply_url"), session=session, timeout=12)
                            if parsed_from_apply.get("job_description_html"):
                                parsed["job_description_html"] = parsed_from_apply["job_description_html"]
                            if parsed_from_apply.get("company_description_html"):
                                parsed["company_description_html"] = parsed_from_apply["company_description_html"]
                            parsed.setdefault("job_url", ext.get("apply_url"))
                        except Exception:
                            pass
                else:
                    parsed.setdefault("notes", []).append("missing_job_id_for_external")
                    parsed["has_external_application"] = 1

                external_parsed.append({"job_meta": job, "parsed": parsed})
                last_external_pos = processed + offset

            if len(internal_parsed) >= n_internal and len(external_parsed) >= n_external:
                break

            if processed >= hard_cap:
                break

        offset += page_limit

        if processed >= hard_cap:
            break

        if allow_delay:
            time.sleep(random.uniform(0.05, 0.25))

    if n_internal == 0:
        internal_offset = 0
    else:
        internal_offset = (last_internal_pos + 1) if last_internal_pos is not None else (offset + 1)

    if n_external == 0:
        external_offset = 0
    else:
        external_offset = (last_external_pos + 1) if last_external_pos is not None else (offset + 1)

    return {
        "internal_parsed": internal_parsed,
        "external_parsed": external_parsed,
        "processed": processed,
        "hard_cap": hard_cap,
        "internal_offset": internal_offset,
        "external_offset": external_offset,
        "batches_fetched": batches,
    }

if __name__ == "__main__":
    res = collect_internal_and_external(
        n_internal=1,
        n_external=1,
        keyword="accountent",
        country="united arab emirates",
        job_type="Full-time",
        location="dubai",
        page_limit=25,
        session=None,
        verbose=True
    )
    print("internal:", len(res["internal_parsed"]), "external:", len(res["external_parsed"]))
