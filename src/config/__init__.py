import os
from typing import Union
from .dev import DevConfig
from .prod import ProdConfig
from .base import BaseConfig, BusinessRules, DataQualityThresholds


def get_config() -> Union[DevConfig, ProdConfig]:
    """Get configuration based on ENVIRONMENT variable."""
    env = os.environ.get("ENVIRONMENT", "dev").lower()

    if env == "prod":
        return ProdConfig()
    else:
        return DevConfig()


def get_config_for_env(env: str) -> Union[DevConfig, ProdConfig]:
    """Get configuration for a specific environment."""
    if env.lower() == "prod":
        return ProdConfig()
    else:
        return DevConfig()


__all__ = [
    "get_config",
    "get_config_for_env",
    "BaseConfig",
    "DevConfig",
    "ProdConfig",
    "BusinessRules",
    "DataQualityThresholds",
]