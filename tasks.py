# tasks.py
"""
Resilient job-scraping dispatcher + workers.
Cleaned and reorganized version of original file with short inline docs.
"""

import time
import uuid
import json
import math
import hashlib
import logging
import redis
import unicodedata
import os

from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse
from collections import OrderedDict, deque
from contextlib import contextmanager
from typing import Dict, Any, Optional, List, Tuple

from celery import shared_task
from sqlalchemy import select, update, func, text, Column, Integer, String, Table, MetaData
from sqlalchemy.exc import IntegrityError, SQLAlchemyError, DatabaseError
from sqlalchemy.orm import joinedload, Session

# local imports (assumes these modules exist in your project)
from config import config
from score import call_llm, calculate_match_score
from models import (
    SessionLocal,
    JobSearch,
    JobResult,
    JobWork,
    Users as User,
    init_db,
    engine as models_engine,
)
from scrapers import scraping_jobs, supports_platform

# initialize DB / app state
init_db()

# ---------- logging ----------
log = logging.getLogger("tasks")
logging.basicConfig(level=logging.INFO)

# ---------- concurrency / redis ----------
_redis = redis.Redis.from_url(os.getenv("REDIS_URL", ""))
DISPATCH_LOCK_KEY = "dispatch_lock_v1"
DISPATCH_LOCK_TTL = 20  # seconds

# ---------- tuning knobs ----------
TOTAL_WORKERS = 3
FAST_RESERVED = 1
SLOW_RESERVED = 1

MAX_JOBWORK_CHUNK = 25
FAST_PHASE_CAP = 20
SLOW_PHASE_ROUND_SIZE = 50
CONSECUTIVE_NO_NEW_PAGES_TO_EXHAUST = 6
MAX_JOBWORK_ATTEMPTS = 5
BACKOFF_SCHEDULE_MINUTES = [5, 10, 20, 40, 60]

# rate limiter for external LLM (example)
OPENROUTER_PER_MINUTE = 10
OPENROUTER_PER_DAY = 1000
RL_MINUTE_KEY = "rate:openrouter:minute"
RL_DAY_KEY = "rate:openrouter:day"

# lightweight selectable for legacy resume table (works for sqlite/postgres)
_metadata = MetaData()
t_resume = Table(
    "resume",
    _metadata,
    Column("user_token", String),
    Column("resume_id", String),
    Column("latex_code", String),
    Column("json_data", String),
    Column("job_name", String),
    Column("resume_name", String),
    Column("is_dupe", Integer),
    Column("is_archive", Integer),
    Column("profile_photo_url", String),
    Column("static_json_data", String),
    Column("template_id", Integer),
    Column("compare_json", String),
)


# ---------- helpers ----------
def redis_acquire_lock(key: str, ttl: int = 10) -> Optional[str]:
    """Try to acquire a simple Redis lock. Return token if acquired."""
    token = str(uuid.uuid4())
    ok = _redis.set(key, token, nx=True, ex=ttl)
    return token if ok else None


def redis_release_lock(key: str, token: Optional[str]) -> None:
    """Release Redis lock only if token matches (safe release)."""
    if not token:
        return
    try:
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        _redis.eval(script, 1, key, token)
    except Exception:
        try:
            _redis.delete(key)
        except Exception:
            pass


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def _clean_text(s: Optional[str]) -> str:
    """Normalize and lower-case text for uid creation and comparisons."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = " ".join(s.split())
    return s.strip().lower()


def compute_unique_id(user_token: str, title: str, company: str, source: str, job_url: str) -> str:
    """Stable SHA256 unique id using normalized fields and canonicalized URL."""
    url = (job_url or "").strip()
    try:
        p = urlparse(url)
        scheme = p.scheme or "https"
        netloc = (p.netloc or "").lower()
        path = (p.path or "").rstrip("/")
        url_norm = urlunparse((scheme, netloc, path, "", "", ""))
    except Exception:
        url_norm = url or ""
    raw = f"{(user_token or '').strip()}|{_clean_text(title)}|{_clean_text(company)}|{_clean_text(source)}|{url_norm}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _read_platform_offsets(js: JobSearch, platform: str) -> Dict[str, int]:
    """
    Normalize offsets saved on JobSearch.
    Returns dict like {'internal': int, 'external': int}.
    """
    offs = js.offsets or {}
    val = offs.get(platform)
    if isinstance(val, dict):
        return {"internal": int(val.get("internal", 0)), "external": int(val.get("external", 0))}
    try:
        ival = int(val or 0)
        return {"internal": ival, "external": ival}
    except Exception:
        return {"internal": 0, "external": 0}


def choose_start_offset(internal: int, external: int) -> int:
    """Pick canonical start offset for pagination (min if both > 0)."""
    try:
        i = int(internal or 0)
        e = int(external or 0)
    except Exception:
        return 0
    if i > 0 and e > 0:
        return min(i, e)
    return i or e or 0


def compute_quotas_from_percentages(platform_percentages: Dict[str, int], total: int) -> Dict[str, int]:
    """Given platform percentage mapping, compute integer quotas that sum to total."""
    platforms = list((platform_percentages or {}).keys())
    if not platforms:
        return {}
    total_pct = sum(platform_percentages.values()) or 1
    quotas = {p: int(round(total * platform_percentages.get(p, 0) / total_pct)) for p in platforms}
    allocated = sum(quotas.values())
    # fix small rounding errors
    while allocated < total:
        best = max(platforms, key=lambda p: platform_percentages.get(p, 0))
        quotas[best] += 1
        allocated += 1
    while allocated > total:
        worst = min(platforms, key=lambda p: platform_percentages.get(p, 0))
        if quotas[worst] > 0:
            quotas[worst] -= 1
            allocated -= 1
        else:
            break
    return quotas


def split_into_chunks(n: int, chunk_size: int = MAX_JOBWORK_CHUNK) -> List[int]:
    """Split n into list of chunk sizes (<= chunk_size)."""
    out: List[int] = []
    remaining = int(n or 0)
    while remaining > 0:
        out.append(min(chunk_size, remaining))
        remaining -= out[-1]
    return out


def merge_offsets(existing_offsets: Dict[str, Any], new_offsets: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge offsets carefully:
     - dict returned by an API scraper overwrites platform dict (preserving unknown keys)
     - scalar offsets keep the max to avoid regression
    """
    merged = (existing_offsets.copy() if existing_offsets else {}).copy()
    for plat, val in (new_offsets or {}).items():
        if isinstance(val, dict):
            prev = merged.get(plat)
            if isinstance(prev, dict):
                combined = prev.copy()
                combined.update({k: int(v or 0) for k, v in val.items()})
                merged[plat] = combined
            else:
                merged[plat] = {k: int(v or 0) for k, v in val.items()}
        else:
            prev = int(merged.get(plat, 0) or 0)
            cur = int(val or 0)
            merged[plat] = max(prev, cur)
    return merged


def count_inserted_for_platform(session: Session, js: JobSearch, platform: str) -> int:
    """Count JobResult rows for a platform; prefer backup_web marker then fallback to source text."""
    try:
        if not platform:
            return 0
        backup = (platform[:2] if platform else None)
        if backup:
            cnt = session.query(JobResult).filter(
                JobResult.search_id == js.search_id,
                JobResult.backup_web == backup,
            ).count()
            if cnt:
                return cnt
        q2 = session.query(JobResult).filter(
            JobResult.search_id == js.search_id,
            JobResult.source.ilike(f"%{platform}%"),
        )
        return q2.count()
    except Exception:
        return 0


def pending_requested_for_platform(session: Session, search_id: str, platform: str) -> int:
    """Sum requested_count for jobworks not yet completed/failed/cancelled for a platform."""
    try:
        val = session.query(func.coalesce(func.sum(JobWork.requested_count), 0)).filter(
            JobWork.search_id == search_id,
            JobWork.platform == platform,
            JobWork.status.in_(["pending", "in_progress", "paused"]),
        ).scalar() or 0
        return int(val)
    except Exception:
        return 0


def platform_paused_with_retry(session: Session, js: JobSearch, platform: str) -> bool:
    """Return True if there's at least one paused JobWork for this platform with backoff in future and attempts left."""
    now = datetime.utcnow()
    paused_jws = session.query(JobWork).filter(
        JobWork.search_id == js.search_id,
        JobWork.platform == platform,
        JobWork.status == "paused",
        JobWork.backoff_until != None,
    ).all()
    for pj in paused_jws:
        if pj.backoff_until and pj.backoff_until > now and (pj.attempts or 0) < len(BACKOFF_SCHEDULE_MINUTES):
            return True
    return False


# ---------- jobwork creation ----------


def create_jobworks_for_js(session: Session, js: JobSearch, phase: str) -> List[JobWork]:
    """
    Create JobWork rows for a JobSearch and phase.
    - 'fast' creates up to FAST_PHASE_CAP; 'slow' up to SLOW_PHASE_ROUND_SIZE.
    - Never exceed per-platform remaining target.
    """
    remaining_total_global = max(0, (js.job_count or 0) - (js.jobs_completed or 0))
    if remaining_total_global <= 0:
        return []

    platform_percentages = js.platform_percentages or {}

    # filter supported platforms
    supported_platforms = {}
    unsupported = []
    for p, pct in (platform_percentages or {}).items():
        if supports_platform(p):
            supported_platforms[p] = pct
        else:
            unsupported.append(p)

    if unsupported:
        log.info("create_jobworks_for_js: skipping unsupported platforms for search %s: %s", js.search_id, unsupported)
    if not supported_platforms:
        return []

    batch_size = FAST_PHASE_CAP if phase == "fast" else SLOW_PHASE_ROUND_SIZE
    batch = min(batch_size, remaining_total_global)

    platforms = list(supported_platforms.keys())
    total_pct = sum(supported_platforms.values()) or 1

    platform_goal_total = {
        p: int(round((js.job_count or 0) * (supported_platforms.get(p, 0) / total_pct)))
        for p in platforms
    }

    # compute how many still needed per platform
    needed = {}
    for p in platforms:
        inserted = count_inserted_for_platform(session, js, p)
        already_scheduled = pending_requested_for_platform(session, js.search_id, p)
        remain_for_p = max(0, platform_goal_total.get(p, 0) - inserted - already_scheduled)
        needed[p] = remain_for_p

    if sum(needed.values()) == 0:
        return []

    # proportional allocation capped by needed
    quotas = {p: 0 for p in platforms}
    for p in platforms:
        prop = int(round(batch * (supported_platforms.get(p, 0) / total_pct)))
        quotas[p] = min(prop, needed[p])

    allocated = sum(quotas.values())
    remain = batch - allocated
    if remain > 0:
        candidates = sorted(platforms, key=lambda x: (-supported_platforms.get(x, 0), -needed.get(x, 0)))
        idx = 0
        while remain > 0 and any(needed[p] > quotas[p] for p in candidates):
            p = candidates[idx % len(candidates)]
            if quotas[p] < needed[p]:
                quotas[p] += 1
                remain -= 1
            idx += 1
            if idx > len(candidates) * 20:
                break

    # trim in case of rounding overflow
    total_alloc = sum(quotas.values())
    while total_alloc > batch:
        worst = min(platforms, key=lambda p: (supported_platforms.get(p, 0), quotas.get(p, 0)))
        if quotas[worst] > 0:
            quotas[worst] -= 1
            total_alloc -= 1
        else:
            break

    jobworks: List[JobWork] = []
    for plat, cnt in quotas.items():
        if cnt <= 0:
            continue
        chunks = split_into_chunks(cnt, MAX_JOBWORK_CHUNK)
        for c in chunks:
            so = _read_platform_offsets(js, plat)
            jw = JobWork(
                search_id=js.search_id,
                platform=plat,
                phase=phase,
                requested_count=c,
                status="pending",
                start_offset=so,
            )
            session.add(jw)
            jobworks.append(jw)
            log.info("create_jobworks_for_js: created jobwork search=%s platform=%s phase=%s requested=%s start_offset=%s",
                     js.search_id, plat, phase, c, so)
    return jobworks


# ---------- scheduler helpers ----------
def round_robin_schedule_pending_jobworks(session: Session, phase: str, scheduling_budget: int) -> int:
    """
    Schedule up to scheduling_budget pending jobworks across searches in round-robin.
    Moves jobworks -> in_progress and enqueues Celery tasks.
    """
    if scheduling_budget <= 0:
        return 0

    pending = session.query(JobWork).join(JobSearch, JobWork.search_id == JobSearch.search_id).filter(
        JobWork.status == "pending", JobWork.phase == phase
    ).order_by(JobSearch.created_at, JobWork.created_at).all()

    if not pending:
        return 0

    groups: Dict[str, deque] = OrderedDict()
    for jw in pending:
        groups.setdefault(jw.search_id, deque()).append(jw)
    search_ids = list(groups.keys())

    scheduled = 0
    i = 0
    while scheduled < scheduling_budget and any(groups.values()):
        sid = search_ids[i % len(search_ids)]
        qdeque = groups.get(sid)
        if qdeque:
            jw = qdeque.popleft()
            try:
                jw.status = "in_progress"
                jw.worker_id = str(uuid.uuid4())
                jw.updated_at = datetime.utcnow()
                session.commit()
            except Exception:
                session.rollback()
                i += 1
                continue
            log.info("[dispatch] scheduling jobwork %s (search=%s platform=%s phase=%s)", jw.id, jw.search_id, jw.platform, jw.phase)
            qname = "fast_scrape" if phase == "fast" else "slow_scrape"
            process_job_work.apply_async(args=(jw.id,), queue=qname)
            scheduled += 1
        i += 1
    return scheduled


# ---------- dispatcher task ----------
@shared_task(name="dispatch_scrapes")
def dispatch_scrapes() -> None:
    """Main dispatcher: promote states, create jobworks, schedule tasks, and redistribute when needed."""
    lock_token = redis_acquire_lock(DISPATCH_LOCK_KEY, ttl=DISPATCH_LOCK_TTL)
    if not lock_token:
        log.info("dispatch_scrapes: another dispatcher is running; exiting.")
        return

    try:
        try:
            if models_engine is not None:
                try:
                    models_engine.dispose()
                except Exception as e:
                    log.warning("dispatch_scrapes: engine.dispose() failed: %s", e)
        except Exception:
            pass

        with session_scope() as session:
            # promote queued -> fast_pending (simple batch update)
            try:
                session.execute(update(JobSearch).where(JobSearch.status == "queued").values(status="fast_pending"))
                session.commit()
            except DatabaseError as dbe:
                log.exception("Database error promoting queued jobs -> fast_pending; disposing engine.")
                if models_engine is not None:
                    try:
                        models_engine.dispose()
                    except Exception:
                        pass
                raise

            # refresh people ahead ordering
            active_states = ["queued", "fast_pending", "fast_in_progress", "fast_running", "slow_pending", "slow_in_progress", "slow_running"]
            jobs_ordered = session.query(JobSearch).filter(JobSearch.status.in_(active_states)).order_by(JobSearch.created_at).all()
            for pos, job in enumerate(jobs_ordered):
                job.people_ahead = pos
            session.commit()

            # re-awaken paused jobworks whose backoff expired
            now = datetime.utcnow()
            paused_jws = session.query(JobWork).filter(JobWork.status == "paused", JobWork.backoff_until != None).all()
            for jw in paused_jws:
                try:
                    if jw.backoff_until and jw.backoff_until <= now:
                        jw.status = "pending"
                        jw.backoff_until = None
                except Exception:
                    pass
            session.commit()

            # prepare fast phase jobworks
            stmt_fast = select(JobSearch).options(joinedload(JobSearch.user)).where(JobSearch.status == "fast_pending").order_by(JobSearch.created_at).limit(FAST_RESERVED)
            fast_jobs = session.execute(stmt_fast).scalars().all()
            for js in fast_jobs:
                log.info("[dispatch] prepare fast jobworks for %s", js.search_id)
                existing_jw = session.query(JobWork).filter(JobWork.search_id == js.search_id, JobWork.phase == "fast").first()
                if not existing_jw:
                    create_jobworks_for_js(session, js, phase="fast")
                    session.commit()
                js.status = "fast_in_progress"
                session.commit()

            # promote fast -> slow if no fast jobworks remain
            in_fast = session.query(JobSearch).filter(JobSearch.status == "fast_in_progress").all()
            for js in in_fast:
                cnt = session.query(JobWork).filter(JobWork.search_id == js.search_id, JobWork.phase == "fast", JobWork.status.in_(["pending", "in_progress", "paused"])).count()
                if cnt == 0:
                    js.status = "slow_pending"
                    created = create_jobworks_for_js(session, js, phase="slow")
                    session.commit()
                    log.info("[dispatch] moved %s to slow_pending and created %s slow jobworks", js.search_id, len(created))

            # prepare slow phase jobworks
            stmt_slow = select(JobSearch).options(joinedload(JobSearch.user)).where(JobSearch.status == "slow_pending").order_by(JobSearch.created_at).limit(SLOW_RESERVED)
            slow_jobs = session.execute(stmt_slow).scalars().all()
            for js in slow_jobs:
                log.info("[dispatch] prepare slow jobworks for %s", js.search_id)
                existing_jw = session.query(JobWork).filter(JobWork.search_id == js.search_id, JobWork.phase == "slow").first()
                if not existing_jw:
                    create_jobworks_for_js(session, js, phase="slow")
                    session.commit()
                js.status = "slow_in_progress"
                session.commit()

            # schedule jobworks (round-robin)
            fast_scheduled = round_robin_schedule_pending_jobworks(session, phase="fast", scheduling_budget=FAST_RESERVED)
            slow_scheduled = round_robin_schedule_pending_jobworks(session, phase="slow", scheduling_budget=SLOW_RESERVED * 2)
            log.debug("dispatch_scrapes scheduled fast=%s slow=%s", fast_scheduled, slow_scheduled)

            # attempt redistribution from exhausted platforms
            js_list = session.query(JobSearch).filter(JobSearch.status.in_(["fast_in_progress", "slow_in_progress", "fast_pending", "slow_pending"])).all()
            for js in js_list:
                exhausted_map = js.platform_exhausted or {}
                if not exhausted_map:
                    continue
                for plat, exhausted in exhausted_map.items():
                    if not exhausted:
                        continue
                    pending_for_plat = session.query(JobWork).filter(
                        JobWork.search_id == js.search_id, JobWork.platform == plat,
                        JobWork.status.in_(["pending", "in_progress", "paused"])
                    ).count()
                    if pending_for_plat > 0:
                        continue

                    total_pct = sum(js.platform_percentages.values() or [1]) or 1
                    target_for_plat = int(round((js.job_count or 0) * (js.platform_percentages.get(plat, 0) / total_pct)))
                    inserted_for_plat = count_inserted_for_platform(session, js, plat)
                    need = max(0, target_for_plat - inserted_for_plat)
                    overall_remaining = max(0, (js.job_count or 0) - (session.query(func.count(JobResult.id)).filter(JobResult.search_id == js.search_id).scalar() or 0))
                    need = min(need, overall_remaining)
                    if need <= 0:
                        continue

                    recipients = [p for p in (js.platform_percentages or {}).keys() if p != plat and not (js.platform_exhausted or {}).get(p, False)]
                    if not recipients:
                        continue

                    per = max(1, need // len(recipients))
                    for r in recipients:
                        chunks = split_into_chunks(per, MAX_JOBWORK_CHUNK)
                        for c in chunks:
                            so = _read_platform_offsets(js, r)
                            nw = JobWork(
                                search_id=js.search_id,
                                platform=r,
                                phase="slow",
                                requested_count=c,
                                status="pending",
                                start_offset=so
                            )
                            session.add(nw)
                            log.info("dispatch_scrapes: redistributed jobwork to %s start_offset=%s", r, so)
                    session.commit()
                    log.info("dispatch_scrapes redistributed %s jobs from exhausted platform %s for search %s to %s",
                             need, plat, js.search_id, recipients)
    except DatabaseError as dbe:
        log.exception("Low-level DB error in dispatcher; disposing engine.")
        if models_engine is not None:
            try:
                models_engine.dispose()
            except Exception:
                pass
        raise
    finally:
        redis_release_lock(DISPATCH_LOCK_KEY, lock_token)


# ---------- worker: process a jobwork ----------
@shared_task(bind=True, name="process_job_work", max_retries=5, default_retry_delay=60)
def process_job_work(self, jobwork_id: int) -> None:
    """
    Worker that executes a single JobWork:
      - calls scraper
      - inserts JobResult rows
      - merges offsets/stats
      - handles backoff / exhaustion / chaining
    """
    session = SessionLocal()
    try:
        jw = session.query(JobWork).with_for_update(nowait=True).filter_by(id=jobwork_id).one_or_none()
        if not jw:
            log.warning("process_job_work: JobWork %s not found", jobwork_id)
            return

        if jw.status not in ("pending", "paused", "in_progress"):
            log.info("process_job_work: JobWork %s already processed: %s", jobwork_id, jw.status)
            return

        def safe_str(val) -> str:
            try:
                if val is None:
                    return ""
                if isinstance(val, float) and math.isnan(val):
                    return ""
                s = str(val)
                return s.strip() if s is not None else ""
            except Exception:
                return ""

        jw.status = "in_progress"
        jw.worker_id = jw.worker_id or str(uuid.uuid4())
        jw.updated_at = datetime.utcnow()
        session.commit()

        js = session.query(JobSearch).filter_by(search_id=jw.search_id).with_for_update().one_or_none()
        if not js:
            jw.status = "failed"
            jw.last_error = "missing JobSearch"
            session.commit()
            return

        if js.status in ("completed", "error"):
            jw.status = "cancelled"
            session.commit()
            return

        platform = (jw.platform or "").lower()
        if not supports_platform(platform):
            pm = js.platform_exhausted or {}
            pm[platform] = True
            js.platform_exhausted = pm
            jw.status = "failed"
            jw.last_error = "platform scraper unavailable"
            jw.updated_at = datetime.utcnow()
            session.commit()
            return

        platform_offsets = _read_platform_offsets(js, platform)
        jw.start_offset = platform_offsets
        session.commit()

        offset_for_paging = choose_start_offset(platform_offsets.get("internal", 0), platform_offsets.get("external", 0))
        resume_arg = platform_offsets if (platform.startswith("naukri") or platform.startswith("gulftalent")) else None
        offset_arg = offset_for_paging

        # call scraper (may return DataFrame or (df, returned_offsets))
        try:
            df_or_tuple = scraping_jobs(
                site_name=[platform],
                search_term=js.search_term,
                google_search_term=f"{js.search_term} {js.location}",
                location=js.location,
                job_type=js.job_type,
                results_wanted=jw.requested_count,
                offset=offset_arg,
                country_indeed=js.country_indeed,
                description_format="html",
                search_id=js.search_id,
                user_token=js.user_token,
                resume_offsets=resume_arg,
            )
        except Exception as e:
            log.exception("process_job_work: scraper exception for jw=%s plat=%s: %s", jobwork_id, platform, e)
            jw.attempts = (jw.attempts or 0) + 1
            if jw.attempts <= len(BACKOFF_SCHEDULE_MINUTES):
                jw.status = "paused"
                backoff_minutes = BACKOFF_SCHEDULE_MINUTES[jw.attempts - 1]
                jw.backoff_until = datetime.utcnow() + timedelta(minutes=backoff_minutes)
                jw.last_error = str(e)[:1024]
                jw.updated_at = datetime.utcnow()
                session.commit()
                return
            else:
                pm = js.platform_exhausted or {}
                pm[platform] = True
                js.platform_exhausted = pm
                jw.status = "failed"
                jw.last_error = f"retries exhausted: {str(e)[:1024]}"
                jw.updated_at = datetime.utcnow()
                session.commit()
                return

        if isinstance(df_or_tuple, tuple) and len(df_or_tuple) == 2:
            df, returned_offsets = df_or_tuple
        else:
            df = df_or_tuple
            returned_offsets = None

        # Trim dataframe to requested_count if necessary (scrapers may overshoot)
        try:
            if hasattr(df, "shape") and df.shape[0] > (jw.requested_count or 0):
                df = df.iloc[: int(jw.requested_count or 0)].reset_index(drop=True)
        except Exception:
            pass

        if df is None:
            jw.attempts = (jw.attempts or 0) + 1
            if jw.attempts <= len(BACKOFF_SCHEDULE_MINUTES):
                jw.status = "paused"
                backoff_minutes = BACKOFF_SCHEDULE_MINUTES[jw.attempts - 1]
                jw.backoff_until = datetime.utcnow() + timedelta(minutes=backoff_minutes)
                jw.last_error = "scraper returned None"
                jw.updated_at = datetime.utcnow()
                session.commit()
                return
            else:
                pm = js.platform_exhausted or {}
                pm[platform] = True
                js.platform_exhausted = pm
                jw.status = "failed"
                jw.last_error = "scraper returned None and retries exhausted"
                jw.updated_at = datetime.utcnow()
                session.commit()
                return

        # count rows returned
        try:
            rows_returned = int(len(df)) if hasattr(df, "__len__") else 0
        except Exception:
            rows_returned = 0

        jw.pages_fetched = (jw.pages_fetched or 0) + 1
        jw.rows_returned = (jw.rows_returned or 0) + rows_returned

        # insert rows
        page_inserted = 0
        skipped_missing = 0
        skipped_duplicate = 0
        batch_seen_uids = set()
        duplicate_samples = []

        rows_iter = df.itertuples() if hasattr(df, "itertuples") else []
        inserted_ids = []
        for row in rows_iter:
            try:
                title = safe_str(getattr(row, "title", None))
                job_url_candidate = getattr(row, "job_url", None) or getattr(row, "job_url_direct", None)
                job_url = safe_str(job_url_candidate)
                if not title or not job_url:
                    skipped_missing += 1
                    continue

                company = safe_str(getattr(row, "company", None))
                source = safe_str(getattr(row, "source", None)) or platform
                uid = compute_unique_id(js.user_token, title, company, source, job_url)

                if uid in batch_seen_uids:
                    skipped_duplicate += 1
                    if len(duplicate_samples) < 5:
                        duplicate_samples.append({"uid": uid, "job_url": job_url, "title": title})
                    continue

                jr = JobResult(
                    search_id=js.search_id,
                    job_title=title,
                    job_description=getattr(row, "description", None),
                    job_url=job_url,
                    user_token=js.user_token,
                    has_external_application=2,
                    company_name=company,
                    company_url=getattr(row, "company_url", None),
                    posted_date=getattr(row, "date_posted", None),
                    company_description=getattr(row, "company_description", None),
                    apply_url=getattr(row, "job_url_direct", None),
                    backup_web=platform[:2],
                    source=source,
                    location=getattr(row, "location", None) or job_url,
                    unique_id=uid,
                    job_function=getattr(row, "job_function", None),
                    json_data=None,
                )

                try:
                    with session.begin_nested():
                        session.add(jr)
                        session.flush()
                    page_inserted += 1
                    batch_seen_uids.add(uid)
                    inserted_ids.append(jr.id)
                    # enqueue scoring task
                    # score_job.apply_async(args=(jr.id,), queue="score")
                    pass
                except IntegrityError:
                    session.rollback()
                    skipped_duplicate += 1
                    if len(duplicate_samples) < 5:
                        duplicate_samples.append({"uid": uid, "job_url": job_url, "title": title})
                    continue
            except Exception as row_exc:
                log.exception("process_job_work[%s] skipping malformed row for platform=%s: %s", jw.id, platform, row_exc)
                skipped_missing += 1
                continue

        jw.rows_inserted = (jw.rows_inserted or 0) + page_inserted
        jw.duplicates = (jw.duplicates or 0) + skipped_duplicate
        if skipped_missing:
            log.info("process_job_work[%s] platform=%s skipped %s rows (missing title/url or malformed)", jw.id, platform, skipped_missing)
        if skipped_duplicate:
            log.info("process_job_work[%s] platform=%s skipped %s duplicate rows (sample=%s)", jw.id, platform, skipped_duplicate, duplicate_samples)

        # consecutive no-new-pages logic
        if page_inserted == 0:
            jw.consecutive_no_new_pages = (jw.consecutive_no_new_pages or 0) + 1
        else:
            jw.consecutive_no_new_pages = 0

        # merge offsets
        new_offsets_partial = {}
        if returned_offsets and isinstance(returned_offsets, dict):
            new_offsets_partial[platform] = {k: int(returned_offsets.get(k, 0) or 0) for k in returned_offsets}
        else:
            if rows_returned:
                try:
                    new_offsets_partial[platform] = int(offset_arg) + int(rows_returned)
                except Exception:
                    new_offsets_partial[platform] = int(rows_returned)
        js.offsets = merge_offsets(js.offsets or {}, new_offsets_partial)

        # update platform_stats
        ps = js.platform_stats or {}
        pstats = ps.get(platform) or {
            "pages_fetched": 0,
            "rows_returned_total": 0,
            "rows_inserted_total": 0,
            "duplicates_total": 0,
            "consecutive_no_new_pages": 0,
        }
        pstats["pages_fetched"] = int(pstats.get("pages_fetched", 0)) + 1
        pstats["rows_returned_total"] = int(pstats.get("rows_returned_total", 0)) + rows_returned
        pstats["rows_inserted_total"] = int(pstats.get("rows_inserted_total", 0)) + page_inserted
        pstats["duplicates_total"] = int(pstats.get("duplicates_total", 0)) + skipped_duplicate
        if returned_offsets and isinstance(returned_offsets, dict):
            pstats["last_returned_offsets"] = returned_offsets
        pstats["consecutive_no_new_pages"] = int(pstats.get("consecutive_no_new_pages", 0)) + (1 if page_inserted == 0 else 0)
        pstats["found_total"] = int(pstats.get("rows_inserted_total", 0))
        ps[platform] = pstats
        js.platform_stats = ps

        # mark exhausted if too many consecutive no-new-pages
        exhausted_map = js.platform_exhausted or {}
        if pstats["consecutive_no_new_pages"] >= CONSECUTIVE_NO_NEW_PAGES_TO_EXHAUST:
            exhausted_map[platform] = True
            js.platform_exhausted = exhausted_map
            jw.last_error = f"platform exhausted by consecutive no-new pages ({pstats['consecutive_no_new_pages']})"
            log.info("process_job_work[%s] platform %s marked exhausted", jw.id, platform)

        # authoritative recount and finalize progress
        actual_count = session.query(func.count(JobResult.id)).filter(JobResult.search_id == js.search_id).scalar() or 0
        js.jobs_completed = int(actual_count)

        if page_inserted > 0:
            js.stalled_batches = 0
            jw.attempts = 0
            jw.backoff_until = None

        jw.status = "completed"
        jw.end_offset = js.offsets.get(platform)
        jw.updated_at = datetime.utcnow()
        session.commit()
        session.refresh(jw)
        try:
            # inserted_ids variable: list of ints - collected during insertion loop
            if page_inserted and inserted_ids:
                # pass inserted ids to scoring task to avoid re-scoring older rows
                score_jobwork_batch.apply_async(args=(jw.id, inserted_ids), queue="score")
            else:
                # still trigger batch scorer for other cases (legacy fallback)
                score_jobwork_batch.apply_async(args=(jw.id,), queue="score")
        except Exception:
            log.exception("process_job_work: failed to enqueue score jobwork for jw=%s", jw.id)

        # finalize search if done
        if js.jobs_completed >= (js.job_count or 0):
            js.status = "completed"
            js.log = f"completed: found {js.jobs_completed}/{js.job_count}"
            session.commit()
            return

        # If nothing inserted -> schedule backoff
        if page_inserted == 0:
            jw.attempts = (jw.attempts or 0) + 1
            if jw.attempts <= len(BACKOFF_SCHEDULE_MINUTES):
                jw.status = "paused"
                backoff_minutes = BACKOFF_SCHEDULE_MINUTES[jw.attempts - 1]
                jw.backoff_until = datetime.utcnow() + timedelta(minutes=backoff_minutes)
                jw.last_error = jw.last_error or "no new rows"
                jw.updated_at = datetime.utcnow()
                session.commit()
                return
            else:
                pm = js.platform_exhausted or {}
                pm[platform] = True
                js.platform_exhausted = pm
                jw.status = "failed"
                jw.last_error = "no new rows and retries exhausted"
                jw.updated_at = datetime.utcnow()
                session.commit()
                return

        # chaining: create next jobwork if still needed
        pct = js.platform_percentages.get(platform, 0) if js.platform_percentages else 0
        total_pct = sum(js.platform_percentages.values()) or 1
        platform_target = int(round((js.job_count or 0) * (pct / total_pct))) if js.platform_percentages else 0
        if platform_target <= 0:
            platform_target = jw.requested_count or 0

        inserted_so_far = count_inserted_for_platform(session, js, platform)
        scheduled_so_far = pending_requested_for_platform(session, js.search_id, platform)
        need_for_platform = max(0, platform_target - (inserted_so_far + scheduled_so_far))
        overall_remaining = max(0, (js.job_count or 0) - js.jobs_completed)
        next_count = min(MAX_JOBWORK_CHUNK, need_for_platform, overall_remaining)
        if next_count > 0 and not (js.platform_exhausted or {}).get(platform, False):
            next_phase = "slow" if jw.phase == "fast" else jw.phase
            so = _read_platform_offsets(js, platform)
            new_jw = JobWork(
                search_id=js.search_id,
                platform=platform,
                phase=next_phase,
                requested_count=int(next_count),
                status="pending",
                start_offset=so
            )
            session.add(new_jw)
            session.commit()
            log.info("process_job_work[%s] created chained jobwork %s for platform=%s count=%s phase=%s start_offset=%s",
                     jw.id, new_jw.id, platform, next_count, next_phase, so)
            q = "fast_scrape" if next_phase == "fast" else "slow_scrape"
            process_job_work.apply_async(args=(new_jw.id,), queue=q)

    except Exception as exc:
        session.rollback()
        log.exception("process_job_work failed for jobwork_id=%s", jobwork_id)
        try:
            jw = session.query(JobWork).filter_by(id=jobwork_id).one_or_none()
            if jw:
                jw.attempts = (jw.attempts or 0) + 1
                if jw.attempts <= len(BACKOFF_SCHEDULE_MINUTES):
                    jw.status = "paused"
                    backoff_minutes = BACKOFF_SCHEDULE_MINUTES[jw.attempts - 1]
                    jw.backoff_until = datetime.utcnow() + timedelta(minutes=backoff_minutes)
                    jw.last_error = str(exc)[:1024]
                else:
                    jw.status = "failed"
                    jw.last_error = str(exc)[:1024]
                jw.updated_at = datetime.utcnow()
                session.commit()
        except Exception:
            session.rollback()
        raise self.retry(exc=exc)
    finally:
        session.close()


# ---------- scoring helpers ----------
def extract_strict_json(maybe_text: Optional[str]) -> Optional[str]:
    """
    Return strictly the JSON object/array from a possibly noisy string.
    Returns JSON string or None.
    """
    if not maybe_text:
        return None
    s = maybe_text.strip()
    try:
        obj = json.loads(s)
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        pass

    # find first brace/bracket and parse balanced chunk
    start = None
    for i, ch in enumerate(s):
        if ch in "{[":
            start = i
            break
    if start is None:
        return None

    stack = []
    for j in range(start, len(s)):
        ch = s[j]
        if ch in "{[":
            stack.append(ch)
        elif ch in "}]":
            if not stack:
                return None
            top = stack.pop()
            if (top == "{" and ch != "}") or (top == "[" and ch != "]"):
                return None
            if not stack:
                candidate = s[start:j + 1]
                try:
                    obj = json.loads(candidate)
                    return json.dumps(obj, ensure_ascii=False)
                except Exception:
                    # continue scanning (maybe later balanced chunk is valid)
                    pass
    return None


def _get_resume_compare_json(session: Session, jr: JobResult) -> Optional[dict]:
    """
    Fetch resume.compare_json:
      - prefer JobSearch.resume_id
      - fallback to latest compare_json by user_token
    """
    js = session.query(JobSearch).filter(JobSearch.search_id == jr.search_id).one_or_none()
    if not js:
        return None

    if js.resume_id:
        row = session.execute(select(t_resume.c.compare_json).where(t_resume.c.resume_id == js.resume_id)).first()
        if row and row[0]:
            try:
                return json.loads(row[0])
            except Exception:
                return None

    row = session.execute(
        select(t_resume.c.compare_json)
        .where(t_resume.c.user_token == js.user_token)
        .where(t_resume.c.compare_json.isnot(None))
        .order_by(text("ROWID DESC"))
        .limit(1)
    ).first()
    if row and row[0]:
        try:
            return json.loads(row[0])
        except Exception:
            return None
    return None


def _rate_limit_allow(n_calls: int = 1) -> Tuple[bool, int]:
    """Redis-based minute/day rate limiter. Returns (allowed, retry_after_seconds)."""
    now = int(time.time())
    minute_key = f"{RL_MINUTE_KEY}:{now // 60}"
    minute_count = _redis.incr(minute_key)
    if minute_count == 1:
        _redis.expire(minute_key, 70)
    if minute_count > OPENROUTER_PER_MINUTE:
        ttl = _redis.ttl(minute_key)
        return False, max(5, ttl if ttl > 0 else 60)

    day_key = f"{RL_DAY_KEY}:{datetime.utcnow().strftime('%Y%m%d')}"
    day_count = _redis.incrby(day_key, n_calls)
    if day_count == n_calls:
        tomorrow = datetime.utcnow().date() + timedelta(days=1)
        seconds = int((datetime.combine(tomorrow, datetime.min.time()) - datetime.utcnow()).total_seconds())
        _redis.expire(day_key, max(3600, seconds))
    if day_count > OPENROUTER_PER_DAY:
        ttl = _redis.ttl(day_key)
        return False, max(60, ttl if ttl > 0 else 3600)
    return True, 0


def _score_single_job(session: Session, jr: JobResult) -> None:
    """
    Core scoring flow:
      1) get JD JSON via LLM (rate-limited)
      2) fetch resume compare json
      3) compute match score and write sim_score + job_tags
    """
    allowed, wait = _rate_limit_allow(1)
    if not allowed:
        raise RuntimeError(f"rate_limited:{wait}")

    model_outputs = call_llm(jr.job_description or "")
    content = None
    for _, v in model_outputs.items():
        content = v
        break

    jd_json_str = extract_strict_json(content or "")
    if not jd_json_str:
        jd_json = {}
    else:
        jr.json_data = jd_json_str
        jd_json = json.loads(jd_json_str)

    resume_json = _get_resume_compare_json(session, jr) or {}
    try:
        result = calculate_match_score(jd_json, resume_json)
    except Exception as e:
        log.exception("scoring failed for JobResult %s: %s", jr.id, e)
        jr.sim_score = 0
        jr.job_tags = json.dumps({"error": "scoring_failed"})
        return

    score = int(round(float(result.get("score", 0))))
    adv = result.get("advantages", []) or []
    dis = result.get("disadvantages", []) or []

    jr.sim_score = score
    jr.job_tags = json.dumps({"advantages": adv, "disadvantages": dis}, ensure_ascii=False)


@shared_task(bind=True, queue="score", max_retries=10, default_retry_delay=60)
def score_job(self, job_result_id: int) -> None:
    """Celery task wrapper for scoring a single JobResult row."""
    session = SessionLocal()
    try:
        jr = session.get(JobResult, job_result_id)
        if not jr or not jr.job_description:
            return
        try:
            _score_single_job(session, jr)
            session.commit()
        except RuntimeError as rl:
            msg = str(rl)
            if msg.startswith("rate_limited:"):
                retry_after = int(msg.split(":")[1] or "60")
                session.rollback()
                raise self.retry(countdown=min(max(retry_after, 30), 600))
            raise
        except SQLAlchemyError as err:
            session.rollback()
            raise self.retry(exc=err, countdown=60)
    finally:
        session.close()


@shared_task(name="score_pending_jobs", queue="score")
def score_pending_jobs(limit: int = 200):
    """Find job_results needing scoring and enqueue them."""
    session = SessionLocal()
    try:
        q = session.query(JobResult.id).filter(
            (JobResult.json_data.is_(None)) | (JobResult.sim_score <= 50)
        ).order_by(JobResult.created_at.desc()).limit(limit)
        ids = [row.id for row in q.all()]
        for jid in ids:
            score_job.apply_async(args=(jid,), queue="score")
    finally:
        session.close()


@shared_task(name="reconcile_job_counts")
def reconcile_job_counts(limit: int = 500):
    """Reconciliation task to fix jobs_completed drift for active searches."""
    session = SessionLocal()
    try:
        active = session.query(JobSearch).filter(
            JobSearch.status.in_(["fast_pending", "fast_in_progress", "fast_running", "slow_pending", "slow_in_progress", "slow_running"])
        ).limit(limit).all()
        for js in active:
            actual = session.query(func.count(JobResult.id)).filter(JobResult.search_id == js.search_id).scalar() or 0
            if js.jobs_completed != actual:
                log.info("reconcile: fixing jobs_completed for %s from %s -> %s", js.search_id, js.jobs_completed, actual)
                js.jobs_completed = int(actual)
                if actual >= (js.job_count or 0):
                    js.status = "completed"
                    js.log = f"completed (reconciled): found {actual}/{js.job_count}"
        session.commit()
    except Exception:
        session.rollback()
        log.exception("reconcile_job_counts failed")
    finally:
        session.close()


@shared_task(bind=True, name="score_jobwork_batch", queue="score", max_retries=5, default_retry_delay=120)
def score_jobwork_batch(self, jobwork_id: int, job_result_ids: list | None = None) -> None:
    """Score all JobResults for a completed JobWork, respecting rate limits."""
    session = SessionLocal()
    jw = session.get(JobWork, jobwork_id)
    if not jw:
        log.debug("score_jobwork_batch: JobWork %s not found (maybe deleted) — skipping", jobwork_id)
        return
    try:
        # Postgres: current_database(); for sqlite different query
        try:
            row = session.execute(text("SELECT current_database()")).first()
            log.info("score_jobwork_batch: current_database() = %s", row and row[0])
        except Exception:
            # SQLite fallback
            try:
                row = session.execute(text("PRAGMA database_list")).fetchall()
                log.info("score_jobwork_batch: sqlite database_list = %s", row)
            except Exception:
                log.info("score_jobwork_batch: couldn't query DB name")

        # log engine url (may include password so redact in prod)
        try:
            log.info("score_jobwork_batch: engine url = %r", session.bind.engine.url)
        except Exception:
            pass

        # optionally show whether the job_work row exists at SQL level
        exists = session.execute(text("SELECT 1 FROM job_work WHERE id = :id LIMIT 1"), {"id": jobwork_id}).first()
        log.info("score_jobwork_batch: raw SELECT existence for id=%s -> %s", jobwork_id, bool(exists))
    except Exception:
        log.exception("score_jobwork_batch: debug checks failed")


    try:
        jw = session.get(JobWork, jobwork_id)
        if not jw:
            log.warning("score_jobwork_batch: JobWork %s not found", jobwork_id)
            return
        if jw.status != "completed":
            log.info("score_jobwork_batch: JobWork %s not completed yet", jobwork_id)
            return

        if job_result_ids:
            # only score the specific inserted rows for this jobwork
            rows = session.query(JobResult).filter(JobResult.id.in_(job_result_ids)).all()
        else:
            # legacy behavior: score all for the search
            rows = session.query(JobResult).filter(JobResult.search_id == jw.search_id).all()

        for jr in rows:
            if jr.json_data:  # already scored
                continue
            try:
                allowed, wait = _rate_limit_allow(1)
                if not allowed:
                    log.info("score_jobwork_batch: rate-limited, retrying after %ss", wait)
                    raise self.retry(countdown=min(wait, 600))

                model_outputs = call_llm(jr.job_description or "")
                content = next(iter(model_outputs.values()), None)
                jd_json_str = extract_strict_json(content or "")
                if not jd_json_str:
                    log.warning("score_jobwork_batch: failed to parse JD json for JobResult %s", jr.id)
                    continue

                jr.json_data = jd_json_str
                jd_json = json.loads(jd_json_str)
                resume_json = _get_resume_compare_json(session, jr) or {}
                result = calculate_match_score(jd_json, resume_json)
                jr.sim_score = int(round(float(result.get("score", 0))))
                jr.job_tags = json.dumps(
                    {"advantages": result.get("advantages", []), "disadvantages": result.get("disadvantages", [])},
                    ensure_ascii=False,
                )
                session.commit()
                time.sleep(1)  # spacing between API calls
            except Exception as e:
                log.exception("score_jobwork_batch: error scoring JobResult %s", jr.id)
                session.rollback()
                continue
    finally:
        session.close()
