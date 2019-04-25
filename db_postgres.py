from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import *

Base = declarative_base()

engine = create_engine(DB_CONNECTION_STRING)

metadata = declarative_base().metadata.create_all(bind=engine)

Session = sessionmaker(bind=engine, autoflush=False)


class Videos(Base):
    __tablename__ = "licensed_content_uploads"
    id = Column('id', INTEGER, primary_key=True)
    list_id = Column('list_id', INTEGER)
    path = Column('path', TEXT)
    video_id = Column('video_id', INTEGER, unique=True)
    video_type = Column('video_type', VARCHAR(56))
    created_at = Column('created_at', DateTime, server_default=text("sysdate"))
    updated_at = Column('updated_at', DateTime, server_default=text("sysdate"))


def create_session():
    session = Session()
    return session


def insert_video(s, path, video_id, list_id, video_type):
    video = Videos()

    video.video_id = video_id
    video.path = path
    video.list_id = list_id
    video.video_type = video_type

    try:
        s.add(video)
        s.commit()

    except Exception as err:
        s.rollback()
        print(err)
        raise

    finally:
        s.close()
