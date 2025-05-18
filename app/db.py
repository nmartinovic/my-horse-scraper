from sqlmodel import SQLModel, create_engine, Session as SQLModelSession

engine = create_engine("sqlite:///data.sqlite", echo=False)

def init_db():
    SQLModel.metadata.create_all(engine)

def get_session():
    return SQLModelSession(engine)

Session = SQLModelSession(engine)
