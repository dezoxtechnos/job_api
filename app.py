# app.py (API side) - corrected authenticate + minor improvements
import json
import uuid
from datetime import date, timedelta, datetime
from functools import wraps

from flask import Flask, request, jsonify, g, current_app
from sqlalchemy.exc import IntegrityError
from models import SessionLocal, JobSearch, Users, JobResult, JobRequest

app = Flask(__name__)


def authenticate(func):
    """
    Authenticate requests using a one-time JobRequest token passed in X-API-KEY.

    - For POST (submit_scrape): token must exist, not expired, and used==False.
      The decorator atomically marks it used and sets metadata.
    - For GET (status): token must exist and not be expired; used may be True (we allow polling).
    - The decorator loads a Users object and calls the wrapped function with that user as the first argument.
    - The consumed JobRequest is placed on flask.g.job_request and the raw token in g.request_token.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        token = request.headers.get("X-API-KEY")
        if not token:
            return jsonify({"error": "Missing API key"}), 401

        session = SessionLocal()
        try:
            now = datetime.utcnow()

            # Decide whether this call is trying to create the scrape (POST) or just poll (GET)
            if request.method == "POST":
                # require unused token and atomically lock it
                jr = (
                    session.query(JobRequest)
                    .filter(
                        JobRequest.request_uuid == token,
                        JobRequest.used == False,
                        (JobRequest.expires_at == None) | (JobRequest.expires_at > now)
                    )
                    .with_for_update()
                    .one_or_none()
                )
                if not jr:
                    return jsonify({"error": "Invalid or already used/expired token"}), 401

                # Atomically mark as used and save metadata
                jr.used = True
                jr.used_at = now
                jr.client_ip = request.remote_addr
                jr.user_agent = request.headers.get("User-Agent")
                session.add(jr)
                session.commit()

            else:
                # For GET (status) allow used==True as long as token exists and not expired
                jr = (
                    session.query(JobRequest)
                    .filter(
                        JobRequest.request_uuid == token,
                        (JobRequest.expires_at == None) | (JobRequest.expires_at > now)
                    )
                    .one_or_none()
                )
                if not jr:
                    return jsonify({"error": "Invalid or expired token"}), 401

            # Load or resolve the user:
            user = None
            # Prefer explicit user_id stored on JR
            if jr.user_id:
                user = session.query(Users).filter_by(id=jr.user_id).one_or_none()
            else:
                # Try to extract user_token from payload (POST) or query param (GET)
                payload = None
                try:
                    # silent=True avoids raising on non-json body
                    payload = request.get_json(silent=True) or {}
                except Exception:
                    payload = {}
                payload_user_token = payload.get("user_token") or request.args.get("user_token")
                if payload_user_token:
                    user = session.query(Users).filter_by(user_token=payload_user_token).one_or_none()

            if not user:
                return jsonify({"error": "Unable to resolve user for this token"}), 401

            # additional safety: if jr.user_id exists and does not match resolved user, reject
            if jr.user_id and user.id != jr.user_id:
                return jsonify({"error": "Token owner mismatch"}), 403

            # place useful objects on flask.g for handler use
            g.job_request = jr
            g.request_token = token

            # Call wrapped function with user as the first arg (handlers expect 'user')
            return func(user, *args, **kwargs)

        except Exception as exc:
            session.rollback()
            current_app.logger.exception("Error in authenticate: %s", exc)
            return jsonify({"error": "Internal auth error"}), 500
        finally:
            session.close()

    return wrapper


def safe_json_text(val):
    """
    Convert dict/list -> JSON text. If already a JSON string, validate and return.
    If val is None, return None.
    If unparsable, return JSON string of empty dict.
    """
    if val is None:
        return None
    # If it's already a str, validate it's JSON
    if isinstance(val, str):
        try:
            json.loads(val)
            return val
        except Exception:
            # not valid JSON string, fall through to dump
            pass
    try:
        return json.dumps(val, ensure_ascii=False)
    except Exception:
        # last resort: return JSON string for empty object
        return json.dumps({})


@app.route("/api/scrape", methods=["POST"])
@authenticate
def submit_scrape(user):
    """
    Create a JobSearch entry and return search_id.
    Note: decorator guarantees token is valid & consumed and places JobRequest on g.job_request.
    """
    payload = request.get_json(force=True) or {}

    # basic required fields
    search_term = payload.get("search_term")
    location = payload.get("location")
    country_indeed = payload.get("country_indeed")
    job_count = int(payload.get("job_count", 0))
    job_type = payload.get("job_type", "trial")
    task_name = payload.get("task_name")
    resume_id = payload.get("resume_id")
    expected_salary = payload.get("expected_salary")
    walk_in_interview_str = payload.get("walk_in_interview")

    if walk_in_interview_str == 1:
        walk_in_interview = True
    else:
        walk_in_interview = False

    if not search_term or not location or not country_indeed or job_count <= 0:
        return jsonify({"error": "missing required fields (search_term, location, country, job_count)"}), 400

    # keep as dicts (SQLAlchemy JSON column handles it)
    platform_percentages = payload.get("platform_percentages") or {}
    offsets = payload.get("offsets") or {}

    # defaults for other json-like fields
    platform_stats = None
    platform_exhausted = None

    days_needed = (job_count // 50) + 1
    start_date = datetime.now().strftime("%Y-%m-%d")
    deadline = (datetime.now() + timedelta(days=days_needed)).strftime("%Y-%m-%d")
    search_id = str(uuid.uuid4())

    session = SessionLocal()
    try:
        new_js = JobSearch(
            user_id=user.id,
            search_term=search_term,
            location=location,
            country_indeed=country_indeed,
            search_id=search_id,
            resume_id=resume_id,
            task_name=task_name,
            job_count=job_count,
            start_date=start_date,
            deadline=deadline,
            job_type=job_type,
            expected_salary=expected_salary,
            walk_in_interview=walk_in_interview,
            platform_percentages=platform_percentages,
            offsets=offsets,
            jobs_completed=0,
            status="queued",
            people_ahead=0,
            user_token=user.user_token,
            stalled_batches=0,
            platform_stats=platform_stats,
            platform_exhausted=platform_exhausted
        )

        session.add(new_js)
        session.commit()

        # update the JobRequest record to store the search_id (helps auditing and later lookups)
        try:
            jr_session = SessionLocal()
            try:
                jr = jr_session.query(JobRequest).filter_by(request_uuid=g.request_token).one_or_none()
                if jr:
                    jr.search_id = search_id
                    jr_session.add(jr)
                    jr_session.commit()
            finally:
                jr_session.close()
        except Exception:
            current_app.logger.exception("Failed to update JobRequest with search_id (non-fatal)")

        return jsonify({"ok": True, "search_id": search_id}), 201

    except IntegrityError as e:
        session.rollback()
        return jsonify({"error": "Failed to save job_search", "detail": str(e)}), 500
    finally:
        session.close()


@app.route("/api/scrape/status", methods=["GET"])
@authenticate
def scrape_status(user):
    """
    Query params:
      - search_id (required)

    Returns JSON:
      - search_id
      - job_target       (requested total jobs to find)
      - jobs_found       (authoritative count from job_result)
      - jobs_completed_field (mirror of JobSearch.jobs_completed)
      - remaining        (job_target - jobs_found, floored at 0)
      - status           (job search status string)
    """
    search_id = request.args.get("search_id")
    if not search_id:
        return jsonify({"error": "missing search_id query parameter"}), 400

    session = SessionLocal()
    try:
        # ensure the user owns the search (security)
        js = session.query(JobSearch).filter_by(search_id=search_id, user_id=user.id).one_or_none()
        if not js:
            return jsonify({"error": "search_id not found or not owned by this user"}), 404

        # authoritative count from job_result (in case jobs_completed drifts)
        from sqlalchemy import func
        actual_count = session.query(func.count(JobResult.id)).filter(JobResult.search_id == search_id).scalar() or 0
        actual_count = int(actual_count)

        job_target = int(js.job_count or 0)
        remaining = max(0, job_target - actual_count)

        return jsonify({
            "search_id": search_id,
            "job_target": job_target,
            "jobs_found": actual_count,
            "jobs_completed_field": int(js.jobs_completed or 0),
            "remaining": remaining,
            "status": js.status
        }), 200
    finally:
        session.close()


if __name__ == "__main__":
    app.run(debug=True, port=5000)
