"""python -m dashboard.webapp  →  http://127.0.0.1:8000  (PORT and HOST can be changed with environment variables)."""
import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run("dashboard.webapp.app:app", host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8000")))
