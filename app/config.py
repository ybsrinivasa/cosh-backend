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

    # AWS S3
    aws_access_key_id: str = "placeholder"
    aws_secret_access_key: str = "placeholder"
    s3_bucket_media: str = "tene-drs-prod-media"
    s3_region: str = "ap-south-1"
    s3_bucket_url: str = "tene-drs-prod-media.s3.ap-south-1.amazonaws.com"

    # Translation
    google_translate_api_key: str = "placeholder"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
