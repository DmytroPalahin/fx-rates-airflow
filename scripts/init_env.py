"""Create .env from .env.example, generating fresh local secrets.

Usage: uv run python scripts/init_env.py   (or: make env)
Does nothing if .env already exists, so it never overwrites your settings.
"""

from __future__ import annotations

import secrets
from pathlib import Path

from cryptography.fernet import Fernet

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / ".env.example"
TARGET = ROOT / ".env"

GENERATED = {
    "AIRFLOW_FERNET_KEY": lambda: Fernet.generate_key().decode(),
    "AIRFLOW_JWT_SECRET": lambda: secrets.token_urlsafe(32),
}


def main() -> None:
    if TARGET.exists():
        print(".env already exists, leaving it untouched")
        return

    lines = []
    for line in EXAMPLE.read_text().splitlines():
        key = line.split("=", 1)[0]
        lines.append(f"{key}={GENERATED[key]()}" if key in GENERATED else line)
    TARGET.write_text("\n".join(lines) + "\n")
    print("created .env with fresh AIRFLOW_FERNET_KEY and AIRFLOW_JWT_SECRET")


if __name__ == "__main__":
    main()
