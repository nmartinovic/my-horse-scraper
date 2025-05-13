from sqlmodel import Session, select
from app.db import engine
from app.models import Race

with Session(engine) as sess:
    all_races = sess.exec(select(Race)).all()
    print(f"DB contains {len(all_races)} races:")
    for r in all_races[:5]:
        print(r.id, r.unibet_id, r.race_time)
