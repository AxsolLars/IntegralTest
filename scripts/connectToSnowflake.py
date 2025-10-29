import time
import random
import tomli
import snowflake.connector
from pathlib import Path
from typing import Any

import cryptography.hazmat.primitives.serialization as serialization
def load_config() -> dict[str, Any]:
    config_path = Path(__file__).parent.parent / "config/snowflake_config.toml"
    with open(config_path, "rb") as f:
        return tomli.load(f)["connection"]
    
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
    
def connect_to_snowflake():
    cfg = load_config()
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
    conn = connect_to_snowflake()
    print("✅ Successfully connected to Snowflake as", conn.user)
    conn.close()