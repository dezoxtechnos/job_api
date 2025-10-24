# models.py
"""
SQLAlchemy ORM models for job-find API.
Explicit sequences are used for PKs to avoid "NULL id" insert errors
when tables were created previously without SERIAL/IDENTITY defaults.
"""
import uuid
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Date,
    Boolean,
    JSON,
    TIMESTAMP,
    ForeignKey,
    UniqueConstraint,
    Sequence,
    Table,
    MetaData,
    text,
    DateTime
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime, timedelta
from config import config

# Engine / Session
engine = create_engine(config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True, pool_recycle=1800, client_encoding="utf8")
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

# Base
metadata = MetaData()
Base = declarative_base(metadata=metadata)

# Optional: resume table similar to DB one (if your app uses it)
t_resume = Table(
    "resume",
    Base.metadata,
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


# ---------------------------------------------------------------------
# Models with explicit Sequence(...) on primary keys
# ---------------------------------------------------------------------
class Users(Base):
    __tablename__ = "users"

    id = Column(Integer, Sequence("users_id_seq"), primary_key=True, index=True)
    username = Column(String, nullable=False)
    user_token = Column(String, unique=True, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    # match DB1 naming to avoid surprises in app code
    job_search = relationship("JobSearch", back_populates="user", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<User id={self.id} username={self.username}>"

class JobRequest(Base):
    __tablename__ = "job_request"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)   # optional but recommended
    request_uuid = Column(String(36), unique=True, nullable=False)     # same as id as string
    used = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=True)
    used_at = Column(DateTime, nullable=True)
    client_ip = Column(String(64), nullable=True)
    user_agent = Column(String(512), nullable=True)
    search_id = Column(String(64), nullable=True)

class JobSearch(Base):
    __tablename__ = "job_search"

    id = Column(Integer, Sequence("job_search_id_seq"), primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    # search params
    search_term = Column(String, nullable=False)
    location = Column(String, nullable=False)
    country_indeed = Column(String, nullable=False)
    search_id = Column(String, unique=True, nullable=False)

    # bookkeeping
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    archive_status = Column(Integer, default=0)
    log = Column(String, default="Just Started")
    resume_id = Column(String, nullable=True)
    task_name = Column(String, nullable=True)

    # schedule/targets
    job_count = Column(Integer, nullable=False)
    start_date = Column(Date, nullable=True)
    deadline = Column(Date, nullable=True)
    job_type = Column(String, nullable=False)
    expected_salary = Column(String, nullable=True)
    walk_in_interview = Column(Boolean, default=False)

    # platform/progress
    platform_percentages = Column(JSON, nullable=False)
    offsets = Column(JSON, nullable=False)
    jobs_completed = Column(Integer, default=0)
    status = Column(String, nullable=False)
    people_ahead = Column(Integer, default=0)
    user_token = Column(String, nullable=True)

    stalled_batches = Column(Integer, default=0)
    platform_stats = Column(JSON, nullable=True)
    platform_exhausted = Column(JSON, nullable=True)

    # relationships (names aligned with DB1)
    user = relationship("Users", back_populates="job_search")
    job_result = relationship("JobResult", back_populates="search", cascade="all, delete-orphan")
    job_work = relationship("JobWork", back_populates="search", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<JobSearch search_id={self.search_id} term={self.search_term}>"


class JobWork(Base):
    __tablename__ = "job_work"

    id = Column(Integer, Sequence("job_work_id_seq"), primary_key=True, index=True)
    # search_id is the external search_id string (matches your existing FK usage)
    search_id = Column(String, ForeignKey("job_search.search_id", ondelete="CASCADE"), nullable=False)

    platform = Column(String, nullable=False)
    phase = Column(String, nullable=False)
    requested_count = Column(Integer, nullable=False)

    status = Column(String, default="pending")
    attempts = Column(Integer, default=0)
    last_error = Column(String, nullable=True)

    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    start_offset = Column(JSON, nullable=True)
    end_offset = Column(JSON, nullable=True)

    pages_fetched = Column(Integer, default=0)
    rows_returned = Column(Integer, default=0)
    rows_inserted = Column(Integer, default=0)
    duplicates = Column(Integer, default=0)
    consecutive_no_new_pages = Column(Integer, default=0)

    backoff_until = Column(TIMESTAMP, nullable=True)
    worker_id = Column(String, nullable=True)

    # relationship (names matched to DB1)
    search = relationship("JobSearch", back_populates="job_work")

    def __repr__(self):
        return f"<JobWork id={self.id} search_id={self.search_id} platform={self.platform}>"


class JobResult(Base):
    __tablename__ = "job_result"

    id = Column(Integer, Sequence("job_result_id_seq"), primary_key=True, index=True)
    search_id = Column(String, ForeignKey("job_search.search_id", ondelete="CASCADE"), nullable=False)

    job_title = Column(String, nullable=False)
    job_description = Column(String, nullable=True)
    job_url = Column(String, nullable=True)
    status = Column(String, default="Pending")
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    archive_status = Column(Integer, default=0)

    json_data = Column(JSON, nullable=True)
    job_tags = Column(JSON, nullable=True)
    sim_score = Column(Integer, default=50)
    user_token = Column(String, nullable=False)
    has_external_application = Column(Integer, default=0)

    company_name = Column(String, nullable=True)
    company_url = Column(String, nullable=True)
    posted_date = Column(String, nullable=True)
    company_description = Column(String, nullable=True)
    apply_url = Column(String, nullable=True)
    backup_web = Column(String, nullable=True)
    source = Column(String, nullable=True)
    due_date = Column(String, nullable=True)
    location = Column(String, nullable=True)
    unique_id = Column(String, nullable=True)
    job_function = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "user_token",
            "job_title",
            "company_name",
            "source",
            "job_url",
            name="uq_job_per_user",
        ),
    )

    # relationship
    search = relationship("JobSearch", back_populates="job_result")

    def __repr__(self):
        return f"<JobResult id={self.id} title={self.job_title} company={self.company_name}>"


# DB init helper
def init_db() -> None:
    """Create sequences & tables if missing. Safe no-op for existing objects."""
    # Create all sequences implicitly when create_all runs because of Sequence objects on columns
    Base.metadata.create_all(bind=engine)
