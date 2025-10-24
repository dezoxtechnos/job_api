import os

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "change_me")
    SQLALCHEMY_DATABASE_URI = "postgresql://jaseel@localhost:5432/post"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    # SQLALCHEMY_DATABASE_URI = "sqlite:////Users/jaseel/Desktop/AI105/main/database.db"
    # SQLALCHEMY_TRACK_MODIFICATIONS = False
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
    # OPENROUTER_API_KEY="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"
    OPENROUTER_API_KEY="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"
    X_API_KEY='testtoken123'
    FAST_SLOTS = int(os.getenv("FAST_SLOTS", 2))
    SLOW_SLOTS = int(os.getenv("SLOW_SLOTS", 2))


config = Config()

#  postgresql://jaseel@localhost:5432/post postgresql://jobsuser@localhost:5432/jobsdb