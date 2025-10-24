import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from config import config
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import difflib
import time
import numpy as np
from datetime import datetime, timezone
import hashlib
import redis


LLM_MODELS = [
    "mistralai/mistral-small-3.2-24b-instruct:free",
    "meta-llama/llama-3.3-8b-instruct:free",
    "meta-llama/llama-4-maverick:free",
    "mistralai/mistral-small-3.1-24b-instruct:free",
    "google/gemma-3-12b-it:free"
]


def _as_dict(x):
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            parsed = json.loads(x)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    return {}

def _as_list(x):
    if isinstance(x, list):
        return x
    if x is None:
        return []
    if isinstance(x, str):
        try:
            parsed = json.loads(x)
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict):
                return [parsed]
            # if parsed is plain str or number, wrap it
            return [parsed]
        except Exception:
            # plain string -> wrap
            return [x]
    if isinstance(x, dict):
        return [x]
    # fallback: wrap non-list value
    return [x]

def _as_int(x, default=0):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default
    
def serialize(obj: dict | list | None) -> str | None:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return None


def polite_sleep(seconds: float) -> None:
    try:
        if seconds > 0:
            time.sleep(seconds)
    except Exception:
        pass


def _call_single_model(model_name: str, desc: str) -> tuple[str, str]:
    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=config.OPENROUTER_API_KEY
    )

    prompt = """
Extract structured data from the following job description. Return only a valid JSON object with the specified fields. Do not explain or output anything else. Extract only explicitly stated information—do not assume or guess.

Field definitions and constraints:

- "role_name": string (exact job title)
- "experience.no_or_preff_or_req": 2 = required, 1 = preferred, 0 = not mentioned
- "experience.years": number or range as string (e.g., "2", "1-3", "0")
- "education.no_or_preff_or_req": same as above
- "education.level": 
  "0" = Not mentioned, 
  "1" = Schooling, 
  "2" = Bachelor, 
  "3" = Masters, 
  "4" = PhD, 
  "5" = All; use comma-separated values if multiple (e.g., "2,3")
- "education.field": string like "Computer Science" or if not fetch then "null"
- "skills": List of technical and relevant soft skills explicitly mentioned
- "region_restricted": true or false
- "eligible_countries": list of country names or "null"
- "gender_specific": true or false
- "gender": 0 = Not specified/all, 1 = Male, 2 = Female, 3 = Other
- "job_type": 1 = Full-time, 2 = Hybrid, 3 = Remote, 4 = Contract
  ❗️Do NOT use 0 as fallback.
- "salary.is_mentioned": true if numeric salary or range mentioned, else false
- "salary.amount": e.g., "1000 AED", "2000-3000 USD", or "null"
- "language_requirement.no_or_preff_or_req": same pattern (2, 1, 0)
- "language_requirement.language": string or "null"
- "language_requirement.proficiency": string or "null"
- "driving_license_required": true or false
- "application_deadline": DD/MM/YYYY or "null"
- "summary": 1-2 line summary of job based on JD
- "contact_info": email or phone from JD, or "null"
- "urgent_status": true if urgency/ASAP mentioned, else false
- "age": number if specific age limit/range is mentioned, else 0
- "company_info": string from JD or "null"
- "walk_in_interview": true if walk-in mentioned, else false

❗️Return "null" as a string only when allowed. Use `0` as number for fields like age or experience only.

Input:
""" + desc + """

Output:
Only valid JSON
"""


    completion = client.chat.completions.create(
        model=model_name,
        messages=[{"role":"system","content":prompt}],
    )
    return completion.choices[0].message.content or ""

# configure Redis cache (same Redis your Celery uses)
_redis_cache = None
try:
    _redis_cache = redis.Redis.from_url("redis://localhost:6379/0")
except Exception:
    _redis_cache = None

def _cache_get(key: str):
    try:
        if not _redis_cache:
            return None
        v = _redis_cache.get(key)
        return v.decode("utf-8") if v else None
    except Exception:
        return None

def _cache_set(key: str, value: str, ttl: int = 60 * 60 * 24):
    try:
        if not _redis_cache:
            return
        _redis_cache.set(key, value, ex=ttl)
    except Exception:
        pass

def call_llm(desc: str, prefer_first_model: bool = True) -> dict[str, str]:
    """
    Sequential attempt at models, with Redis caching.
    Returns a single successful model -> content mapping (or the best available).
    This avoids making concurrent calls to multiple models per JD.
    """
    # short-circuit empty desc
    if not desc:
        return {}

    # compute stable cache key from description
    key = "llm:jd:" + hashlib.sha256(desc.encode("utf-8")).hexdigest()
    cached = _cache_get(key)
    if cached:
        # return as a single-item dict using a synthetic "cache" key
        return {"cache": cached}

    # try each model in order — return first non-empty response
    for model in LLM_MODELS:
        try:
            content = _call_single_model(model, desc)
            if not content:
                continue
            # keep content even if it might be noisy; tasks.extract_strict_json will validate
            _cache_set(key, content)
            return {model: content}
        except Exception:
            # move to next model
            continue

    # fallback: try all concurrently but only keep first non-error (rare fallback)
    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=min(len(LLM_MODELS), 3)) as exe:
        futures = {exe.submit(_call_single_model, m, desc): m for m in LLM_MODELS}
        for fut in as_completed(futures):
            m = futures[fut]
            try:
                content = fut.result()
                if content:
                    _cache_set(key, content)
                    return {m: content}
            except Exception:
                results[m] = json.dumps({"error": str(e)})
    return results

def parse_range(value):
    try:
        if isinstance(value, (int, float)):
            return value, value
        value = str(value).strip()
        if not value:
            return 0, 0
        if "-" in value:
            parts = value.split("-")
            return int(parts[0]), int(parts[1])
        return int(value), int(value)
    except Exception:
        return 0, 0

def proficiency_label(level):
    labels = {1: "Basic", 2: "Intermediate", 3: "Advanced", 4: "Proficient", 5: "Native"}
    return labels.get(level, "Unknown")

def _as_dict(x):
    """Coerce x into a dict if possible; otherwise return {}."""
    if isinstance(x, dict):
        return x
    if x is None:
        return {}
    if isinstance(x, str):
        s = x.strip()
        # treat "null" / "None" as empty
        if s.lower() in ("null", "none", ""):
            return {}
        try:
            parsed = json.loads(s)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    return {}

def _as_list(x):
    """Coerce x into a list. Strings are json-parsed if possible, otherwise wrapped."""
    if isinstance(x, list):
        return x
    if x is None:
        return []
    if isinstance(x, str):
        s = x.strip()
        if s.lower() in ("null", "none", ""):
            return []
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict):
                return [parsed]
            # parsed might be a primitive — wrap it
            return [parsed]
        except Exception:
            # plain string — wrap
            return [s]
    if isinstance(x, dict):
        return [x]
    # other primitives -> wrap
    return [x]

def _as_int(x, default=0):
    """Try to coerce to int (handles numeric strings, floats)."""
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip()
        if not s:
            return default
        # if contains currency or non-digit, try to extract digits
        if any(c.isalpha() for c in s) and not s.replace("-", "").replace(".", "").isdigit():
            # fallback: strip nondigits
            import re
            digits = re.findall(r"-?\d+", s)
            if digits:
                return int(digits[0])
            return default
        return int(float(s))
    except Exception:
        return default

def _parse_levels_to_ints(value):
    """Turn level field into list of ints (handles '2,3' or [2,3] or '2')."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        out = []
        for v in value:
            try:
                out.append(int(v))
            except Exception:
                pass
        return out
    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() in ("null", "none"):
            return []
        parts = [p.strip() for p in s.split(",") if p.strip()]
        out = []
        for p in parts:
            try:
                out.append(int(float(p)))
            except Exception:
                # try to extract digits
                import re
                m = re.search(r"-?\d+", p)
                if m:
                    out.append(int(m.group()))
        return out
    try:
        return [int(value)]
    except Exception:
        return []

def calculate_match_score(job, resume):
    """
    Robust match scoring: coerces noisy LLM outputs into expected shapes,
    then applies the original weighted scoring logic. Returns the same dict
    structure as before: {"score": final_score, "advantages": [...], "disadvantages": [...], "data": (...)}
    """
    # weights (unchanged)
    weights = {
        "experience": 0.3,
        "education": 0.25,
        "skills": 0.05,
        "salary": 0.1,
        "language": 0.1,
        "driving": 0.1,
        "age_walkin": 0.1
    }

    messages = {"advantages": [], "disadvantages": []}

    # normalize top-level job/resume into dicts
    job = _as_dict(job)
    resume = _as_dict(resume)

    # === EXPERIENCE ===
    years_score = 0.0
    field_score = 0.0
    industry_score = 0.0

    job_exp = _as_dict(job.get("experience", {}))
    resume_exp = _as_dict(resume.get("experience", {}))

    job_exp_required = _as_int(job_exp.get("no_preff_req", 0))
    job_exp_years_raw = job_exp.get("years", "0")
    resume_exp_status = bool(resume_exp.get("experience_status", False))
    resume_exp_years = _as_int(resume_exp.get("years", 0))

    job_exp_min, job_exp_max = parse_range(job_exp_years_raw)

    # Required
    if job_exp_required == 2:
        if not resume_exp_status or resume_exp_years < job_exp_min:
            years_score = 0.0
            messages["disadvantages"].append("The job requires a minimum amount of experience that you do not meet.")
        else:
            denom = (job_exp_max - job_exp_min)
            ratio = 0.0
            if denom <= 0:
                # if job specifies a single number, treat meeting it as full
                ratio = 1.0
            else:
                ratio = min(1.0, (resume_exp_years - job_exp_min) / (denom + 1e-6))
            years_score = 0.8 + 0.2 * ratio
            if resume_exp_years > job_exp_max:
                messages["advantages"].append("You have extra years of experience!")
            else:
                messages["advantages"].append("Your experience meets the requirement well.")
    elif job_exp_required == 1:
        base_score = 0.75
        if resume_exp_status and resume_exp_years >= job_exp_min:
            years_score = 1.0
            messages["advantages"].append("You meet the preferred experience criteria.")
        else:
            diff = max(0, job_exp_min - resume_exp_years)
            penalty = diff * 0.035
            years_score = max(0, base_score - penalty)
            messages["disadvantages"].append("Your experience is slightly below the preferred level.")
    else:
        years_score = 1.0
        messages["advantages"].append("Experience is not a critical factor for this job.")

    # Field comparison
    job_exp_field = str(job_exp.get("field", "") or "").strip().lower()
    resume_exp_field = str(resume_exp.get("field", "") or "").strip().lower()
    if job_exp_field and resume_exp_field:
        try:
            field_similarity = difflib.SequenceMatcher(None, job_exp_field, resume_exp_field).ratio()
            field_score = field_similarity
            if field_similarity < 0.5:
                messages["disadvantages"].append("Your experience field does not align closely with the job requirement.")
            else:
                messages["advantages"].append("Your experience field matches the job requirement.")
        except Exception:
            field_score = 0.5
            messages["disadvantages"].append("Error comparing experience fields.")
    else:
        field_score = 0.5
        messages["disadvantages"].append("Missing information to fully compare the experience field.")

    # Industry comparison
    job_exp_industry = str(job_exp.get("industry", "") or "").strip().lower()
    resume_exp_industry = str(resume_exp.get("industry", "") or "").strip().lower()
    if job_exp_industry and resume_exp_industry:
        try:
            industry_similarity = difflib.SequenceMatcher(None, job_exp_industry, resume_exp_industry).ratio()
            industry_score = industry_similarity
            if industry_similarity < 0.5:
                messages["disadvantages"].append("Your industry experience does not match the job requirement well.")
            else:
                messages["advantages"].append("Your industry experience aligns with the job requirement.")
        except Exception:
            industry_score = 0.5
            messages["disadvantages"].append("Error comparing industry experience.")
    else:
        industry_score = 0.5
        messages["disadvantages"].append("Missing information to compare industry experience.")

    exp_score = (years_score + field_score + industry_score) / 3.0

    # === EDUCATION ===
    edu_score = 1.0
    job_edu = _as_dict(job.get("education", {}))
    resume_edu = _as_dict(resume.get("education", {}))

    job_edu_required = _as_int(job_edu.get("no_preff_req", 0))
    job_edu_levels = _parse_levels_to_ints(job_edu.get("level", "0"))
    job_edu_field = job_edu.get("field", "null")
    resume_edu_level = _as_int(resume_edu.get("level", 0))
    resume_edu_field = str(resume_edu.get("field", "") or "").strip()

    if job_edu_required == 2:
        if resume_edu_level not in job_edu_levels:
            edu_score = 0.0
            messages["disadvantages"].append("Your education level does not match the required level.")
        else:
            messages["advantages"].append("Your education level meets the requirement.")
    elif job_edu_required == 1:
        if resume_edu_level in job_edu_levels:
            messages["advantages"].append("Your education level is a plus for this job.")
        else:
            edu_score *= 0.9
            messages["disadvantages"].append("Your education level is slightly below the preferred level.")
    else:
        messages["advantages"].append("Education is not a key factor for this position.")

    if str(job_edu_field).lower() != "null" and resume_edu_field:
        try:
            edu_field_similarity = difflib.SequenceMatcher(None, str(job_edu_field).lower(), resume_edu_field.lower()).ratio()
            edu_score *= edu_field_similarity
            if edu_field_similarity < 0.5:
                messages["disadvantages"].append("Your field of study does not closely match the job requirement.")
            else:
                messages["advantages"].append("Your field of study aligns well with the job requirement.")
        except Exception:
            messages["disadvantages"].append("Missing information to compare education field.")
    else:
        messages["disadvantages"].append("Missing information to compare education field.")

    # === SKILLS ===
    job_skills = _as_list(job.get("skills", []))
    resume_skills = _as_list(resume.get("skills", []))
    job_skills_str = " ".join([str(s) for s in job_skills]) if job_skills else ""
    resume_skills_str = " ".join([str(s) for s in resume_skills]) if resume_skills else ""
    try:
        if job_skills_str and resume_skills_str:
            vectorizer = CountVectorizer().fit_transform([job_skills_str, resume_skills_str])
            skills_similarity = float(cosine_similarity(vectorizer)[0, 1])
        else:
            skills_similarity = 0.0
    except Exception:
        skills_similarity = 0.0

    skills_score = 0.8 + 0.2 * skills_similarity
    if skills_similarity > 0.5:
        messages["advantages"].append("Your skills match the job requirements well!")
    else:
        messages["disadvantages"].append("There is some room for improvement in your skills match.")

    # === SALARY ===
    salary_score = 1.0
    job_salary = _as_dict(job.get("salary", {}))
    if job_salary.get("is_mentioned", False):
        job_salary_amount = job_salary.get("amount", "0")
        job_min_salary, job_max_salary = parse_range(job_salary_amount)
        resume_salary = resume.get("expected_salary", None)
        resume_salary_num = None if resume_salary is None else _as_int(resume_salary, default=None)
        if resume_salary is None or resume_salary_num is None:
            messages["advantages"].append("No expected salary provided—your flexibility is a plus!")
        else:
            if job_min_salary <= resume_salary_num <= job_max_salary:
                salary_score = 1.0
                messages["advantages"].append("Your salary expectations match the job offer.")
            else:
                diff = 0
                if resume_salary_num < job_min_salary:
                    diff = job_min_salary - resume_salary_num
                elif resume_salary_num > job_max_salary:
                    diff = resume_salary_num - job_max_salary
                try:
                    salary_score = float(1 / (1 + np.exp(diff / 500.0)))
                except Exception:
                    salary_score = 0.5
                messages["disadvantages"].append("Your salary expectation is somewhat off the job's range.")
    else:
        messages["advantages"].append("Salary is not specified for this job.")

    # === LANGUAGE ===
    language_score = 1.0
    job_languages = _as_list(job.get("language_requirement", []))
    resume_languages = _as_list(resume.get("language", []))
    language_factors = []

    for raw_req in job_languages:
        req = _as_dict(raw_req) if not isinstance(raw_req, dict) else raw_req
        req_importance = _as_int(req.get("no_preff_req", 1))
        if req_importance == 0:
            continue
        req_lang = str(req.get("language", "") or "").lower()
        req_prof = _as_int(req.get("proficiency", 1))
        match_found = False

        for raw_cand in resume_languages:
            cand = _as_dict(raw_cand) if not isinstance(raw_cand, dict) else raw_cand
            cand_lang = str(cand.get("language", "") or "").lower()
            if cand_lang == req_lang and req_lang:
                match_found = True
                cand_prof = _as_int(cand.get("proficiency", 0))
                base_factor = min(1.0, (cand_prof / req_prof)) if req_prof else 1.0
                if req_importance == 2:
                    if cand_prof >= req_prof:
                        factor = base_factor
                        messages["advantages"].append(f"Your {req_lang.capitalize()} proficiency meets the required level.")
                    else:
                        factor = base_factor * 0.8
                        messages["disadvantages"].append(f"Your {req_lang.capitalize()} proficiency is below the required level.")
                else:  # preferred
                    if cand_prof >= req_prof:
                        factor = base_factor
                        messages["advantages"].append(f"Your {req_lang.capitalize()} proficiency meets the preferred level.")
                    else:
                        factor = base_factor * 0.95
                        messages["disadvantages"].append(f"Your {req_lang.capitalize()} proficiency is below the preferred level.")
                language_factors.append(float(factor))
                break

        if not match_found:
            if req_importance == 2:
                language_factors.append(0.5)
                messages["disadvantages"].append(f"Missing required language: {req_lang.capitalize()}.")
            elif req_importance == 1:
                language_factors.append(0.9)
                messages["disadvantages"].append(f"Missing preferred language: {req_lang.capitalize()}.")

    if language_factors:
        try:
            avg_lang_factor = float(np.mean(language_factors))
            language_score = 0.5 + 0.5 * avg_lang_factor
        except Exception:
            language_score = 0.75
    else:
        language_score = 1.0
        messages["advantages"].append("No language requirements or all language requirements are optional.")

    # === DRIVING LICENSE ===
    driving_score = 1.0
    job_license_req = _as_int(job.get("driving_licence", job.get("driving_license", 0)))
    resume_license = bool(resume.get("driving_licence", resume.get("driving_license", False)))
    if job_license_req == 2:
        if not resume_license:
            driving_score = 0.0
            messages["disadvantages"].append("A driving license is required but you do not have one.")
        else:
            messages["advantages"].append("You have the required driving license.")
    elif job_license_req == 1:
        if resume_license:
            messages["advantages"].append("You have the preferred driving license.")
        else:
            driving_score = 0.95
            messages["disadvantages"].append("While preferred, a driving license is missing.")

    # === AGE & WALK-IN ===
    age_score = 1.0
    job_age_limit_raw = job.get("age_limit", "0")
    job_min_age, job_max_age = parse_range(job_age_limit_raw)
    candidate_age = _as_int(resume.get("age", 0))
    if job_age_limit_raw and str(job_age_limit_raw) != "0":
        if job_min_age <= candidate_age <= job_max_age:
            age_score = 1.0
            messages["advantages"].append("Your age is within the required range.")
        else:
            age_score = 0.9
            messages["disadvantages"].append(f"Your age ({candidate_age}) is slightly outside the desired range ({job_age_limit_raw}).")
    else:
        messages["advantages"].append("No age restrictions.")

    walkin_score = 1.0
    if bool(job.get("walk_in_interview", False)):
        if bool(resume.get("walk_in_interview", False)):
            walkin_score = 1.0
            messages["advantages"].append("You are available for the walk-in interview.")
        else:
            walkin_score = 0
            messages["disadvantages"].append("Walk-in interview is required but you're not available.")
    else:
        messages["advantages"].append("Walk-in interview not required.")

    age_walkin_score = (age_score + walkin_score) / 2.0

    # === FINAL AGGREGATION ===
    overall_score = (
        weights["experience"] * exp_score +
        weights["education"] * edu_score +
        weights["skills"] * skills_score +
        weights["salary"] * salary_score +
        weights["language"] * language_score +
        weights["driving"] * driving_score +
        weights["age_walkin"] * age_walkin_score
    )

    data = (
        weights["experience"] * exp_score,
        weights["education"] * edu_score,
        weights["skills"] * skills_score,
        weights["salary"] * salary_score,
        weights["language"] * language_score,
        weights["driving"] * driving_score,
        weights["age_walkin"] * age_walkin_score
    )

    final_score = round(float(overall_score) * 100, 2)
    print("THE ADV ARE: ", messages["advantages"])
    return {"score": final_score, "advantages": messages["advantages"], "disadvantages": messages["disadvantages"], "data": data}
