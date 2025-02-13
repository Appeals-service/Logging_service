from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).parent.parent


class Settings(BaseSettings):
    SERVICE_NAME: str = "Notification service"

    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_DEFAULT_USER: str
    RABBITMQ_DEFAULT_PASS: str
    RABBITMQ_QUEUE_NAME: str

    LOGGING_LEVEL: str = "DEBUG"
    LOGGING_JSON: bool = True
    LOGGING_FORMAT: str = "%(asctime)s - %(filename)s - %(levelname)s - %(message)s"

    ECHO: bool = False

    model_config = SettingsConfigDict(env_file=ROOT_DIR / ".env", env_file_encoding="utf-8", extra="allow")

    def get_rmq_url(self) -> str:
        return (f"amqp://{self.RABBITMQ_DEFAULT_USER}:{self.RABBITMQ_DEFAULT_PASS}@"
                f"{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}/")


settings = Settings()
