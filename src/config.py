import os
from pathlib import Path

from dotenv import load_dotenv

raiz_projeto = Path(__file__).resolve().parent.parent
arquivo_env = raiz_projeto / ".env"
if not arquivo_env.is_file():
    arquivo_env = raiz_projeto.parent / ".env"
load_dotenv(arquivo_env)

MONGODB_URI = os.getenv("MONGODB_URI")
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL")
