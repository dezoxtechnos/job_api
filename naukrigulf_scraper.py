# naukrigulf_scraper.py
import requests
import random
import time
import json
import os

from math import floor
from datetime import datetime, timezone
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Any, Tuple
from urllib.parse import urlparse

BASE_API = "https://www.naukrigulf.com/spapi/jobapi/search"
JOB_DETAIL_API = "https://www.naukrigulf.com/spapi/jobs/{job_id}?seo=1&locationId=&nationality=&xz=1_0_5&source=srpTuple"
LISTING_URL_TEMPLATE = "https://www.naukrigulf.com/{kw_slug}-jobs-in-{loc_slug}?offset={offset}"

APPID_POOL = ["200", "201", "202", "203", "204", "205"]
SYSTEMID_POOL = ["2000", "2010", "2222", "2323", "2400"]
USER_AGENT_POOL = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
]
GEO_CITY_POOL = ["Kochi", "Kochi" ]

DEFAULT_TIMEOUT = 20
MAX_LIMIT_PER_REQUEST = 49
RETRY_LIMIT = 3
POLITE_SLEEP = 0.25
DETAIL_CONCURRENCY = 8
OVERFETCH_FACTOR = 1.25
MAX_SCAN_MULTIPLIER = 3.0
MIN_SCAN_THRESHOLD = 100
ABSOLUTE_MAX_SCAN = 2000


def _get_case_insensitive(d: dict, *keys):
    if not isinstance(d, dict):
        return None
    for k in keys:
        for variant in (k, k.lower(), k.upper(), k.title()):
            if variant in d:
                return d[variant]
    for k in keys:
        if k in d and isinstance(d[k], dict):
            nested = d[k]
            for nk in ("Name", "name"):
                if nk in nested:
                    return nested[nk]
    return None

def _detect_is_easy(job_obj: dict) -> Optional[bool]:
    if not isinstance(job_obj, dict):
        return None
    easy = _get_case_insensitive(job_obj, "isEasyApply", "IsEasyApply")
    if easy is not None:
        if isinstance(easy, bool):
            return easy
        s = str(easy).strip().lower()
        if s in ("1", "true", "yes"):
            return True
        if s in ("0", "false", "no"):
            return False
    redirect = _get_case_insensitive(job_obj, "jobRedirection", "JobRedirection", "jobRedirect", "jobRedir")
    if redirect is not None:
        s = str(redirect).strip().lower()
        if s in ("1", "true", "yes"):
            return False
        if s in ("0", "false", "no"):
            return True
    form = _get_case_insensitive(job_obj, "IsFormBasedApply", "isFormBasedApply")
    if form is not None:
        s = str(form).strip().lower()
        if s in ("1", "true", "yes"):
            return True
    return None

def _normalize_listing_jobs(raw_data: dict) -> list:
    out = []
    if not raw_data:
        return out
    jobs_container = raw_data.get("jobs") or raw_data.get("Jobs")
    if jobs_container is None:
        for v in raw_data.values():
            if isinstance(v, list):
                jobs_container = v
                break
    if not jobs_container:
        return out
    for item in jobs_container:
        j = item.get("Job") if isinstance(item, dict) and "Job" in item else item
        job_id = _get_case_insensitive(j, "jobId", "JobId")
        jd_url = _get_case_insensitive(j, "jdURL") or ""
        description = _get_case_insensitive(j, "description") or ""
        is_easy = _detect_is_easy(j)
        out.append({
            "jobId": str(job_id) if job_id is not None else None,
            "jdURL": jd_url,
            "description": description,
            "isEasyApply": is_easy,
            "raw": j
        })
    return out

def _save_json_atomic(data, path):
    # Keep helper in case future code wants it, but we don't auto-save debug files now.
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


class NaukriGulfScraper:
    def __init__(
        self,
        appid_pool: List[str] = None,
        systemid_pool: List[str] = None,
        user_agent_pool: List[str] = None,
        geo_city_pool: List[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        self.appid_pool = appid_pool or APPID_POOL
        self.systemid_pool = systemid_pool or SYSTEMID_POOL
        self.user_agent_pool = user_agent_pool or USER_AGENT_POOL
        self.geo_city_pool = geo_city_pool or GEO_CITY_POOL
        self.timeout = timeout
        self.session = requests.Session()

    def _random_headers(self, referer: Optional[str] = None) -> dict:
        appid = random.choice(self.appid_pool)
        systemid = random.choice(self.systemid_pool)
        ua = random.choice(self.user_agent_pool)
        headers = {
            "accept": "application/json",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "appid": appid,
            "systemid": systemid,
            "client-type": "desktop",
            "clientid": "desktop",
            "device-type": "desktop",
            "user-agent": ua,
            "referer": referer or "https://www.naukrigulf.com",
        }
        return headers

    def _listing_referer(self, keywords: str, location: str, offset: int) -> str:
        kw_slug = keywords.replace(" ", "-").lower()
        loc_slug = location.replace(" ", "-").lower()
        return LISTING_URL_TEMPLATE.format(kw_slug=kw_slug, loc_slug=loc_slug, offset=offset)

    def _convert_posted_date(self, ts_str: str) -> str:
        try:
            ts = int(ts_str)
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.isoformat()
        except Exception:
            return ""

    def _safe_get_json(self, url: str, params: dict = None, headers: dict = None, cookies: dict = None) -> Optional[dict]:
        attempt = 0
        while attempt < RETRY_LIMIT:
            try:
                resp = self.session.get(url, params=params, headers=headers, cookies=cookies, timeout=self.timeout)
                resp.raise_for_status()
                try:
                    return resp.json()
                except Exception:
                    return None
            except (ReadTimeout, ConnectionError) as e:
                attempt += 1
                delay = 1 + attempt * 2
                time.sleep(delay)
            except HTTPError:
                return None
            except Exception:
                return None
        return None

    def _fetch_listing_batch(self, keywords: str, location: str, offset: int, limit: int, geo_city: str, geo_country: str = "India") -> Tuple[Optional[dict], list]:
        params = {
            "Experience": "",
            "Keywords": keywords,
            "KeywordsAr": "",
            "Limit": str(limit),
            "Location": location,
            "LocationAr": "",
            "Offset": str(offset),
            "SortPreference": "",
            "breadcrumb": "1",
            "geoIpCityName": geo_city,
            "geoIpCountryName": geo_country,
            "locationId": "",
            "nationality": "",
            "nationalityLabel": "",
            "pageNo": str(floor(max(1, offset) / max(1, limit)) + 1),
            "seo": "1",
            "showBellyFilters": "true",
            "srchId": "",
            "topEmployer": "true",
        }
        referer = self._listing_referer(keywords, location, offset)
        headers = self._random_headers(referer=referer)
        try:
            self.session.get(referer, headers=headers, timeout=self.timeout)
        except Exception:
            pass

        js = self._safe_get_json(BASE_API, params=params, headers=headers)
        if js is None:
            return None, []
        jobs = _normalize_listing_jobs(js)
        return js, jobs

    def _fetch_job_detail_for_external(self, job_id: str, referer_jdurl: Optional[str] = None) -> Optional[dict]:
        if not job_id:
            return None
        url = JOB_DETAIL_API.format(job_id=job_id)
        if referer_jdurl:
            referer = referer_jdurl if referer_jdurl.startswith("http") else f"https://www.naukrigulf.com/{referer_jdurl}"
        else:
            referer = f"https://www.naukrigulf.com/some-jd-jid-{job_id}"

        headers = {
            "accept": "application/json",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "appid": "205",
            "cache-control": "no-cache",
            "client-type": "desktop",
            "clientid": "desktop",
            "device-type": "desktop",
            "referer": referer,
            "systemid": "2323",
            "userdata": "7|IN",
            "version": "v1",
            "user-agent": random.choice(self.user_agent_pool),
        }

        cookies = {
            "countryc": "IN",
            "countryn": "India",
            "city": "Kochi",
            "state": "Kerala",
            "pwa_lang": "en",
            "_ngenv1[lang]": "en"
        }

        try:
            try:
                self.session.get(referer, headers=self._random_headers(referer=referer), timeout=6)
            except Exception:
                pass
            resp = self.session.get(url, headers=headers, cookies=cookies, timeout=self.timeout)
            resp.raise_for_status()
            try:
                return resp.json()
            except Exception:
                return None
        except Exception:
            return None

    def _extract_external_apply_link(self, job_detail_json: dict) -> Optional[str]:
        if not job_detail_json:
            return None
        def find_http_in_str(s: str) -> Optional[str]:
            if not isinstance(s, str):
                return None
            for proto in ("http://", "https://"):
                idx = s.find(proto)
                if idx != -1:
                    cand = s[idx:]
                    for sep in ('"', "'", " ", "<", ">", ",", ")"):
                        if sep in cand:
                            cand = cand.split(sep, 1)[0]
                    return cand.strip()
            return None

        def extract_from_small_obj(o: dict) -> Optional[str]:
            if not isinstance(o, dict):
                return None
            for k in ("url", "link", "href", "website", "Website", "Url"):
                v = o.get(k)
                if isinstance(v, str) and v.strip():
                    return (find_http_in_str(v) or v.strip())
            return None

        def find_url(obj):
            if isinstance(obj, str):
                v = find_http_in_str(obj)
                if v:
                    return v
                try:
                    parsed = json.loads(obj)
                    return find_url(parsed)
                except Exception:
                    return None
            if isinstance(obj, dict):
                for key in ("tag", "Tag", "externalUrl", "externalApplyUrl", "applyUrl", "redirectUrl"):
                    if key in obj:
                        r = find_url(obj[key])
                        if r:
                            return r
                job_obj = obj.get("Job") or obj.get("job") or obj
                if isinstance(job_obj, dict):
                    other = job_obj.get("Other") or job_obj.get("other")
                    if isinstance(other, dict):
                        tag = other.get("Tag") or other.get("tag")
                        if tag:
                            if isinstance(tag, str):
                                r = find_http_in_str(tag) or find_url(tag)
                                if r:
                                    return r
                            else:
                                r = find_url(tag) or extract_from_small_obj(tag)
                                if r:
                                    return r
                    contact = job_obj.get("Contact") or job_obj.get("contact")
                    if isinstance(contact, dict):
                        site = contact.get("Website") or contact.get("website")
                        if site:
                            r = find_http_in_str(site) or (site.strip() if isinstance(site, str) else None)
                            if r:
                                return r
                ex = extract_from_small_obj(obj)
                if ex:
                    return ex
                for v in obj.values():
                    r = find_url(v)
                    if r:
                        return r
            if isinstance(obj, list):
                for it in obj:
                    r = find_url(it)
                    if r:
                        return r
            return None

        found = find_url(job_detail_json)
        if found:
            return found.strip()
        return None

    def _normalize_job(self, job_json: dict, external_apply_url: Optional[str] = None, is_easy: Optional[bool] = None) -> dict:
        company_block = job_json.get("company") or job_json.get("Company") or {}
        company_name = company_block.get("name") or company_block.get("Name") or ""
        company_url = ""
        comp_slug = company_block.get("url") or ""
        if isinstance(comp_slug, str) and comp_slug:
            if comp_slug.startswith("http"):
                company_url = comp_slug
            else:
                company_url = f"https://www.naukrigulf.com/{comp_slug}"

        jdURL = job_json.get("jdURL") or job_json.get("JdURL") or job_json.get("Jdurl") or ""
        jd_full = jdURL if (isinstance(jdURL, str) and jdURL.startswith("http")) else (f"https://www.naukrigulf.com/{jdURL}" if jdURL else "")

        internal_apply = jd_full
        external_apply = external_apply_url or None

        if is_easy is True and not external_apply:
            external_apply = internal_apply

        apply_url = external_apply or internal_apply or ""

        source = ""
        try:
            if apply_url:
                host = urlparse(apply_url).netloc or ""
                if host:
                    parts = host.split(".")
                    source = parts[-2] if len(parts) >= 2 else host
        except Exception:
            source = ""

        desc_html = _get_case_insensitive(job_json, "description", "Description", "jobInfo", "JobInfo", "JobDescription", "jobDescription") or ""
        lp = job_json.get("latestPostedDate") or job_json.get("LatestPostedDate") or ""
        posted = self._convert_posted_date(lp) if lp else ""

        return {
            "company_name": company_name,
            "company_url": company_url,
            "posted_date": posted,
            "due_date": "",
            "job_description_html": desc_html,
            "is_easy_apply": is_easy,
            "internal_apply_url": internal_apply,
            "external_apply_url": external_apply,
            "apply_url": apply_url,
            "company_about": "",
            "source": source
        }

    def scrape(
        self,
        search_term: str,
        location: str,
        total_easy: int,
        total_external: int,
        resume_offsets: Optional[Dict[str, int]] = None,
        overfetch_factor: float = OVERFETCH_FACTOR,
        max_scan_multiplier: float = MAX_SCAN_MULTIPLIER,
        min_scan_threshold: int = MIN_SCAN_THRESHOLD,
        absolute_max_scan: int = ABSOLUTE_MAX_SCAN,
    ) -> Dict[str, Any]:
        required_easy = int(total_easy)
        required_external = int(total_external)
        required_total = required_easy + required_external
        if required_total <= 0:
            return {"easy_jobs": [], "external_jobs": [], "offsets": {"easy": 0, "external": 0, "global": 0}, "processed": 0}

        scan_limit = max(50, required_total * 2)
        scan_limit = min(scan_limit, absolute_max_scan)

        resume_easy = resume_offsets.get("easy", 0) if resume_offsets else 0
        resume_external = resume_offsets.get("external", 0) if resume_offsets else 0
        global_offset = min(resume_easy or 0, resume_external or 0)

        start_global_offset = global_offset
        processed = 0
        easy_next_offset = None
        external_next_offset = None

        candidates: List[dict] = []
        seen_jids = set()
        fetch_records: Dict[str, dict] = {}
        detail_attempts = []
        listing_originals: List[dict] = []
        patched_count = 0

        while processed < scan_limit and len(candidates) < scan_limit:
            limit = min(MAX_LIMIT_PER_REQUEST, scan_limit - processed)
            if limit <= 0:
                break

            geo_city = random.choice(self.geo_city_pool)
            js, jobs = self._fetch_listing_batch(search_term, location, global_offset, limit, geo_city)
            if js is None:
                time.sleep(1)
                break

            raw_items = js.get("jobs") or js.get("Jobs") or []
            if isinstance(raw_items, list) and raw_items:
                listing_originals.extend(raw_items)

            if not jobs:
                break

            for entry in jobs:
                processed += 1
                global_offset += 1
                absolute_pos = global_offset

                jid = entry.get("jobId")
                if not jid or jid in seen_jids:
                    continue
                seen_jids.add(jid)

                is_easy_flag = entry.get("isEasyApply")
                if is_easy_flag is None:
                    is_easy = None
                else:
                    if isinstance(is_easy_flag, bool):
                        is_easy = is_easy_flag
                    else:
                        s = str(is_easy_flag).strip().lower()
                        is_easy = True if s in ("1", "true", "yes") else False if s in ("0", "false", "no") else None

                raw_job = entry.get("raw") or {}
                try:
                    raw_job["isEasyApply"] = is_easy
                except Exception:
                    pass

                candidates.append({
                    "raw": raw_job,
                    "is_easy_listing": is_easy,
                    "pos": absolute_pos,
                    "jobId": str(jid)
                })

                fetch_records[str(jid)] = {
                    "jobId": str(jid),
                    "pos": absolute_pos,
                    "is_easy_listing": is_easy,
                    "listing_raw": raw_job,
                    "detail_fetched": False,
                    "detail_json": None,
                    "extracted_external": None,
                    "accepted_external": False,
                    "patched_into_easy": False
                }

                if len(candidates) >= scan_limit:
                    break

            time.sleep(POLITE_SLEEP)

        easy_reserved = candidates[:required_easy]
        easy_reserved_raw = [c["raw"] for c in easy_reserved]
        last_easy_pos = easy_reserved[-1]["pos"] if easy_reserved else (start_global_offset)
        remaining = candidates[required_easy:]

        prioritized = []
        for c in remaining:
            if c["is_easy_listing"] is False:
                prioritized.append(c)
        for c in remaining:
            if c["is_easy_listing"] is None:
                prioritized.append(c)

        def _is_real_external_url(u: str) -> bool:
            if not u or not isinstance(u, str):
                return False
            u = u.strip()
            lower = u.split("?", 1)[0].lower()
            img_exts = (".gif", ".png", ".jpg", ".jpeg", ".svg", ".ico", ".webp")
            if any(lower.endswith(ext) for ext in img_exts):
                return False
            try:
                p = urlparse(u)
                host = (p.netloc or "").lower()
                if any(k in host for k in ("naukri", "naukrigulf", "naukimg")):
                    return False
                if "." not in host:
                    return False
                return True
            except Exception:
                return False

        external_results: List[dict] = []
        external_positions: List[int] = []
        if prioritized:
            with ThreadPoolExecutor(max_workers=DETAIL_CONCURRENCY) as ex:
                future_to_c = {}
                for c in prioritized:
                    jid = c["jobId"]
                    raw = c["raw"]
                    jd = raw.get("jdURL") or raw.get("JdURL") or raw.get("Jdurl") or ""
                    referer_jd = jd if (isinstance(jd, str) and jd.startswith("http")) else (f"https://www.naukrigulf.com/{jd}" if jd else None)
                    future = ex.submit(self._fetch_job_detail_for_external, jid, referer_jd)
                    future_to_c[future] = c

                for fut in as_completed(future_to_c):
                    c = future_to_c[fut]
                    jid = c["jobId"]
                    raw = c["raw"]
                    pos = c.get("pos")
                    try:
                        detail_json = fut.result()
                    except Exception:
                        detail_json = None

                    fetch_rec = fetch_records.get(jid)
                    if fetch_rec is not None:
                        fetch_rec["detail_fetched"] = True
                        fetch_rec["detail_json"] = detail_json

                    detail_attempts.append({"jobId": jid, "pos": pos, "detail": detail_json})

                    external_url = None
                    detail_is_easy = None
                    if detail_json:
                        external_url = self._extract_external_apply_link(detail_json)
                        job_block = detail_json.get("Job") or detail_json.get("job") or detail_json
                        detail_is_easy = _detect_is_easy(job_block)
                        if detail_is_easy is None:
                            detail_is_easy = False if (external_url and _is_real_external_url(external_url)) else True

                    if external_url and not external_url.startswith("http"):
                        external_url = "https://" + external_url.lstrip("/")

                    jd_raw = raw.get("jdURL") or raw.get("JdURL") or raw.get("Jdurl") or ""
                    jd_full = jd_raw if (isinstance(jd_raw, str) and jd_raw.startswith("http")) else (f"https://www.naukrigulf.com/{jd_raw}" if jd_raw else "")

                    if external_url and jd_full and external_url.rstrip("/") == jd_full.rstrip("/"):
                        external_url = None

                    if external_url and not _is_real_external_url(external_url):
                        external_url = None

                    if external_url:
                        external_results.append(self._normalize_job(raw, external_apply_url=external_url, is_easy=False))
                        external_positions.append(pos)
                        if fetch_rec is not None:
                            fetch_rec["extracted_external"] = external_url
                            fetch_rec["accepted_external"] = True
                    else:
                        if fetch_rec is not None:
                            fetch_rec["extracted_external"] = None
                            fetch_rec["accepted_external"] = False

                    if len(external_results) >= required_external:
                        break

        if len(external_results) < required_external:
            shortage = required_external - len(external_results)
            appended = 0
            idx = 0
            used_internal_urls = set()
            for e in easy_reserved_raw:
                jd_e = e.get("jdURL") or e.get("JdURL") or e.get("Jdurl") or ""
                jd_full_e = jd_e if (isinstance(jd_e, str) and jd_e.startswith("http")) else (f"https://www.naukrigulf.com/{jd_e}" if jd_e else "")
                if jd_full_e:
                    used_internal_urls.add(jd_full_e.rstrip("/"))

            while appended < shortage and idx < len(remaining):
                cand = remaining[idx]
                raw = cand["raw"]
                pos = cand["pos"]
                jid = cand["jobId"]
                jd_c = raw.get("jdURL") or raw.get("JdURL") or raw.get("Jdurl") or ""
                jd_full_c = jd_c if (isinstance(jd_c, str) and jd_c.startswith("http")) else (f"https://www.naukrigulf.com/{jd_c}" if jd_c else "")
                idx += 1
                if jd_full_c and jd_full_c.rstrip("/") in used_internal_urls:
                    continue
                easy_reserved_raw.append(raw)
                used_internal_urls.add(jd_full_c.rstrip("/") if jd_full_c else "")
                last_easy_pos = pos
                appended += 1
                patched_count += 1
                rec = fetch_records.get(jid)
                if rec is not None:
                    rec["patched_into_easy"] = True
                    rec["patched_pos"] = pos

        easy_results = [ self._normalize_job(j, external_apply_url=None, is_easy=True) for j in easy_reserved_raw ]

        if easy_reserved_raw:
            easy_next_offset = (last_easy_pos + 1) if last_easy_pos else (start_global_offset + 1)
        else:
            easy_next_offset = start_global_offset + 1

        fallback_next = start_global_offset + processed + 1
        if external_positions:
            external_next_offset = max(external_positions) + 1
        else:
            external_next_offset = fallback_next

        global_next_offset = min(easy_next_offset, external_next_offset)

        offsets = {
            "easy": int(easy_next_offset),
            "external": int(external_next_offset),
            "global": int(global_next_offset)
        }

        result = {
            "easy_jobs": easy_results,
            "external_jobs": external_results[:required_external],
            "offsets": offsets,
            "processed": processed,
            "scan_limit": scan_limit,
            "patched_into_easy": int(patched_count),
        }
        return result

# quick local test
if __name__ == "__main__":
    sc = NaukriGulfScraper()
    out = sc.scrape("Accountant", "Dubai", total_easy=2, total_external=2)
    print("easy:", len(out["easy_jobs"]), "external:", len(out["external_jobs"]), "processed:", out["processed"])
