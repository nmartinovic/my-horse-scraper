# init_db.py
from app.db import init_db
from app.models import Race, RaceDetail  # 👈 ensure models are imported!

init_db()
print("✅ Database initialized.")
