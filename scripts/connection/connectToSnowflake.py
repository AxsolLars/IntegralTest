import time
import random
import tomli
import snowflake.connector
from snowflake.snowpark import Session
from pathlib import Path
from typing import Any

import cryptography.hazmat.primitives.serialization as serialization
def load_config(connection_name) -> dict[str, Any]:
    config_path = Path(__file__).parent.parent.parent / "config/snowflake_config.toml"
    with open(config_path, "rb") as f:
        return tomli.load(f)[connection_name]
    
def get_private_key(path: str):
    with open(path, "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=None,
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
def create_session():
    cfg = load_config("dev-bok")
    private_key = get_private_key(cfg["private_key_path"])
    
    cfg["authenticator"] = "SNOWFLAKE_JWT"
    cfg["private_key"] = private_key
    return Session.builder.configs(cfg).create()

def connect_to_testing():
    cfg = load_config("testing")
    p_key = get_private_key(cfg["private_key_path"])
    return snowflake.connector.connect(
        user=cfg["user"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
        role=cfg["role"],
        private_key=p_key
    )
    
def connect_to_bok():
    cfg = load_config("dev-bok")
    p_key = get_private_key(cfg["private_key_path"])
    return snowflake.connector.connect(
        user=cfg["user"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
        role=cfg["role"],
        private_key=p_key
    )
if __name__ == "__main__":
    conn = connect_to_testing()
    print("✅ Successfully connected to Snowflake as", conn.user)
    conn.close()