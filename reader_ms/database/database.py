from sqlalchemy import  create_engine
from sqlalchemy.orm import sessionmaker, declarative_base




db_url = "postgresql://postgres:postgres@localhost:5432/ssv"

engine = create_engine(db_url,echo=True)

Base = declarative_base()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        #  the yield stops the function
        yield db
    finally:
        db.close()


Base.metadata.create_all(engine)


