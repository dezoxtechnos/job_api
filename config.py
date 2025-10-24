import os

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "change_me")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REDIS_URL = os.getenv("REDIS_URL", "")
    MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
    OPENROUTER_API_KEY=os.getenv("OPENROUTER_API_KEY", "")
    X_API_KEY=os.getenv("X_API_KEY", "")
    FAST_SLOTS = int(os.getenv("FAST_SLOTS", 2))
    SLOW_SLOTS = int(os.getenv("SLOW_SLOTS", 2))

config = Config()
