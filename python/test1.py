import sqlalchemy
from sqlalchemy import create_engine,text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
Base = declarative_base()
engine = create_engine('sqlite:///:memory:', echo=True)
Session = sessionmaker(bind=engine)

session = Session()

class User(Base):
    __tablename__ = "user"
    id = Column(Integer,primary_key=True)
    name = Column(String(50))
    fullname  = Column(String(50))
    nickname  = Column(String(50))

    def __repr__(self):
       return "<User(name='%s', fullname='%s', nickname='%s')>" % (self.name, self.fullname, self.nickname)


Base.metadata.create_all(engine)
ed_user = User(name='ed', fullname='Ed Jones', nickname='edsnickname')
session.add(ed_user)
session.add_all([
    User(name='wendy', fullname='Wendy Williams', nickname='windy'),
    User(name='mary', fullname='Mary Contrary', nickname='mary'),
    User(name='fred', fullname='Fred Flintstone', nickname='freddy')])
our_user = session.query(User).filter_by(name='ed').first()
print(our_user.id)
for row in session.query(User).filter(text("id<=2")).all():
    print(row.name)