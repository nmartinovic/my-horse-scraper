from app.db import Session
from app.models import Race
from sqlmodel import select

with Session as session:
    races = session.exec(select(Race)).all()
    for race in races:
        print(race)
