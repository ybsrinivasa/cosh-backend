from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str
    database_url_sync: str
    neo4j_uri: str
    neo4j_username: str
    neo4j_password: str
    redis_url: str
    secret_key: str
    admin_email: str
    admin_password: str
    environment: str = "development"
    cors_origins: str = "http://localhost:3000"

    # AWS S3
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    s3_bucket_media: str = ""
    s3_region: str = ""
    s3_bucket_url: str = ""

    # Translation
    google_translate_api_key: str = "placeholder"

    # Email / OTP
    email_smtp_host: str = "smtp.gmail.com"
    email_smtp_port: int = 587
    email_smtp_user: str = ""
    email_smtp_pass: str = ""
    email_from: str = ""
    email_reply_to: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
