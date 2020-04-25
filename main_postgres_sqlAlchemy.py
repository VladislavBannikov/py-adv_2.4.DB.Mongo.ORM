import csv
from datetime import datetime
from sqlalchemy import create_engine, Integer, Column, Float, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()
engine = create_engine('postgresql://netology_user:1@localhost/netology')
Session = sessionmaker(bind=engine)
session = Session()


class Artist(Base):
    __tablename__ = 'artist'
    id = Column(Integer, primary_key=True)
    name = Column(String(30),nullable=False)
    price = Column(Integer, nullable=False)
    place = Column(String(30), nullable=False)
    date = Column(Date, nullable=False)

    def __str__(self):
        return "{:>25}{:>7}{:>30}{:  %Y-%m-%d}".format(self.name,self.price,self.place,self.date)


def read_data():
    """импорт данных из csv файла;"""
    with open("artists.csv", encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=",")
        artists = list(reader)
    header = artists.pop(0)

    for art in artists:
        art_row = Artist(name=art[0], price=art[1], place=art[2], date=datetime.strptime(f"{art[3]}.2020", '%d.%m.%Y'))
        session.add(art_row)
    session.commit()


def find_cheapest():
    """отсортировать билеты из базы по возрастанию цены;"""
    return session.query(Artist).order_by(Artist.price).all()


def find_by_name(name):
    """найти билеты по исполнителю, где имя исполнителя может быть задано не полностью, и вернуть их по возрастанию цены."""
    return session.query(Artist).filter(Artist.name.like(f'%{name}%')).order_by(Artist.price).all()


if __name__ == '__main__':
    # Base.metadata.create_all(engine)
    # read_data()


    for i in find_cheapest():
        print(i)
    print("==========")
    for i in find_by_name('а'):
        print(i)




