from datetime import datetime

from marshy.marshaller import NoOpMarshaller
from schemey.number_schema import NumberSchema
from schemey.obj_schema import ObjSchema

from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.obj_storage.attr import Attr
from persisty.obj_storage.stored import stored
from persisty.storage.field.field_type import FieldType

SUPER_BOWL_RESULT_DICTS = [
    {'code': 'i', 'year': 1967, 'date': '1967-01-15', 'winner_code': 'green_bay', 'runner_up_code': 'kansas_city',
     'winner_score': 35, 'runner_up_score': 10},
    {'code': 'ii', 'year': 1968, 'date': '1968-01-14', 'winner_code': 'green_bay', 'runner_up_code': 'oakland',
     'winner_score': 33, 'runner_up_score': 14},
    {'code': 'iii', 'year': 1969, 'date': '1969-01-12', 'winner_code': 'new_york_jets', 'runner_up_code': 'baltimore',
     'winner_score': 16, 'runner_up_score': 7},
    {'code': 'iv', 'year': 1970, 'date': '1970-01-11', 'winner_code': 'kansas_city', 'runner_up_code': 'minnesota',
     'winner_score': 23, 'runner_up_score': 7},
    {'code': 'v', 'year': 1971, 'date': '1971-01-17', 'winner_code': 'baltimore', 'runner_up_code': 'dallas',
     'winner_score': 16, 'runner_up_score': 13},
    {'code': 'vi', 'year': 1972, 'date': '1972-01-16', 'winner_code': 'dallas', 'runner_up_code': 'miami',
     'winner_score': 24, 'runner_up_score': 3},
    {'code': 'vii', 'year': 1973, 'date': '1973-01-14', 'winner_code': 'miami', 'runner_up_code': 'washington',
     'winner_score': 14, 'runner_up_score': 7},
    {'code': 'viii', 'year': 1974, 'date': '1974-01-13', 'winner_code': 'miami', 'runner_up_code': 'minnesota',
     'winner_score': 24, 'runner_up_score': 7},
    {'code': 'ix', 'year': 1975, 'date': '1975-01-12', 'winner_code': 'pittsburgh', 'runner_up_code': 'minnesota',
     'winner_score': 16, 'runner_up_score': 6},
    {'code': 'x', 'year': 1976, 'date': '1976-01-18', 'winner_code': 'pittsburgh', 'runner_up_code': 'dallas',
     'winner_score': 21, 'runner_up_score': 17},
    {'code': 'xi', 'year': 1977, 'date': '1977-01-09', 'winner_code': 'oakland', 'runner_up_code': 'minnesota',
     'winner_score': 32, 'runner_up_score': 14},
    {'code': 'xii', 'year': 1978, 'date': '1978-01-15', 'winner_code': 'dallas', 'runner_up_code': 'denver',
     'winner_score': 27, 'runner_up_score': 10},
    {'code': 'xiii', 'year': 1979, 'date': '1979-01-21', 'winner_code': 'pittsburgh', 'runner_up_code': 'dallas',
     'winner_score': 35, 'runner_up_score': 31},
    {'code': 'xiv', 'year': 1980, 'date': '1980-01-20', 'winner_code': 'pittsburgh',
     'runner_up_code': 'los_angeles_rams', 'winner_score': 31, 'runner_up_score': 19},
    {'code': 'xv', 'year': 1981, 'date': '1981-01-25', 'winner_code': 'oakland', 'runner_up_code': 'philadelphia',
     'winner_score': 27, 'runner_up_score': 10},
    {'code': 'xvi', 'year': 1982, 'date': '1982-01-24', 'winner_code': 'san_francisco',
     'runner_up_code': 'cincinnati', 'winner_score': 26, 'runner_up_score': 21},
    {'code': 'xvii', 'year': 1983, 'date': '1983-01-30', 'winner_code': 'washington', 'runner_up_code': 'miami',
     'winner_score': 27, 'runner_up_score': 17},
    {'code': 'xviii', 'year': 1984, 'date': '1984-01-22', 'winner_code': 'los_angeles_raiders',
     'runner_up_code': 'washington', 'winner_score': 38, 'runner_up_score': 9},
    {'code': 'xix', 'year': 1985, 'date': '1985-01-20', 'winner_code': 'san_francisco', 'runner_up_code': 'miami',
     'winner_score': 38, 'runner_up_score': 16},
    {'code': 'xx', 'year': 1986, 'date': '1986-01-26', 'winner_code': 'chicago', 'runner_up_code': 'new_england',
     'winner_score': 46, 'runner_up_score': 10},
    {'code': 'xxi', 'year': 1987, 'date': '1987-01-25', 'winner_code': 'new_york_giants', 'runner_up_code': 'denver',
     'winner_score': 39, 'runner_up_score': 20},
    {'code': 'xxii', 'year': 1988, 'date': '1988-01-31', 'winner_code': 'washington', 'runner_up_code': 'denver',
     'winner_score': 42, 'runner_up_score': 10},
    {'code': 'xxiii', 'year': 1989, 'date': '1989-01-22', 'winner_code': 'san_francisco',
     'runner_up_code': 'cincinnati', 'winner_score': 20, 'runner_up_score': 16},
    {'code': 'xxiv', 'year': 1990, 'date': '1990-01-28', 'winner_code': 'san_francisco', 'runner_up_code': 'denver',
     'winner_score': 55, 'runner_up_score': 10},
    {'code': 'xxv', 'year': 1991, 'date': '1991-01-27', 'winner_code': 'new_york_giants', 'runner_up_code': 'buffalo',
     'winner_score': 20, 'runner_up_score': 19},
    {'code': 'xxvi', 'year': 1992, 'date': '1992-01-26', 'winner_code': 'washington', 'runner_up_code': 'buffalo',
     'winner_score': 37, 'runner_up_score': 24},
    {'code': 'xxvii', 'year': 1993, 'date': '1993-01-31', 'winner_code': 'dallas', 'runner_up_code': 'buffalo',
     'winner_score': 52, 'runner_up_score': 17},
    {'code': 'xxviii', 'year': 1994, 'date': '1994-01-30', 'winner_code': 'dallas', 'runner_up_code': 'buffalo',
     'winner_score': 30, 'runner_up_score': 13},
    {'code': 'xxix', 'year': 1995, 'date': '1995-01-29', 'winner_code': 'san_francisco',
     'runner_up_code': 'san_diego', 'winner_score': 49, 'runner_up_score': 26},
    {'code': 'xxx', 'year': 1996, 'date': '1996-01-28', 'winner_code': 'dallas', 'runner_up_code': 'pittsburgh',
     'winner_score': 27, 'runner_up_score': 17},
    {'code': 'xxxi', 'year': 1997, 'date': '1997-01-26', 'winner_code': 'green_bay', 'runner_up_code': 'new_england',
     'winner_score': 35, 'runner_up_score': 21},
    {'code': 'xxxii', 'year': 1998, 'date': '1998-01-25', 'winner_code': 'denver', 'runner_up_code': 'green_bay',
     'winner_score': 31, 'runner_up_score': 24},
    {'code': 'xxxiii', 'year': 1999, 'date': '1999-01-31', 'winner_code': 'denver', 'runner_up_code': 'atlanta',
     'winner_score': 34, 'runner_up_score': 19},
    {'code': 'xxxiv', 'year': 2000, 'date': '2000-01-30', 'winner_code': 'st._louis', 'runner_up_code': 'tennessee',
     'winner_score': 23, 'runner_up_score': 16},
    {'code': 'xxxv', 'year': 2001, 'date': '2001-01-28', 'winner_code': 'baltimore',
     'runner_up_code': 'new_york_giants', 'winner_score': 34, 'runner_up_score': 7},
    {'code': 'xxxvi', 'year': 2002, 'date': '2002-02-03', 'winner_code': 'new_england', 'runner_up_code': 'st._louis',
     'winner_score': 20, 'runner_up_score': 17},
    {'code': 'xxxvii', 'year': 2003, 'date': '2003-01-26', 'winner_code': 'tampa_bay', 'runner_up_code': 'oakland',
     'winner_score': 48, 'runner_up_score': 21},
    {'code': 'xxxviii', 'year': 2004, 'date': '2004-02-01', 'winner_code': 'new_england',
     'runner_up_code': 'carolina', 'winner_score': 32, 'runner_up_score': 29},
    {'code': 'xxxix', 'year': 2005, 'date': '2005-02-06', 'winner_code': 'new_england',
     'runner_up_code': 'philadelphia', 'winner_score': 24, 'runner_up_score': 21},
    {'code': 'xl', 'year': 2006, 'date': '2006-02-05', 'winner_code': 'pittsburgh', 'runner_up_code': 'seattle',
     'winner_score': 21, 'runner_up_score': 10},
    {'code': 'xli', 'year': 2007, 'date': '2007-02-04', 'winner_code': 'indianapolis', 'runner_up_code': 'chicago',
     'winner_score': 29, 'runner_up_score': 17},
    {'code': 'xlii', 'year': 2008, 'date': '2008-02-03', 'winner_code': 'new_york_giants',
     'runner_up_code': 'new_england', 'winner_score': 17, 'runner_up_score': 14},
    {'code': 'xliii', 'year': 2009, 'date': '2009-02-01', 'winner_code': 'pittsburgh', 'runner_up_code': 'arizona',
     'winner_score': 27, 'runner_up_score': 23},
    {'code': 'xliv', 'year': 2010, 'date': '2010-02-07', 'winner_code': 'new_orleans',
     'runner_up_code': 'indianapolis', 'winner_score': 31, 'runner_up_score': 17},
    {'code': 'xlv', 'year': 2011, 'date': '2011-02-06', 'winner_code': 'green_bay', 'runner_up_code': 'pittsburgh',
     'winner_score': 31, 'runner_up_score': 25},
    {'code': 'xlvi', 'year': 2012, 'date': '2012-02-05', 'winner_code': 'new_york_giants',
     'runner_up_code': 'new_england', 'winner_score': 21, 'runner_up_score': 17},
    {'code': 'xlvii', 'year': 2013, 'date': '2013-02-03', 'winner_code': 'baltimore',
     'runner_up_code': 'san_francisco', 'winner_score': 34, 'runner_up_score': 31},
    {'code': 'xlviii', 'year': 2014, 'date': '2014-02-02', 'winner_code': 'seattle', 'runner_up_code': 'denver',
     'winner_score': 43, 'runner_up_score': 8},
    {'code': 'xlix', 'year': 2015, 'date': '2015-02-01', 'winner_code': 'new_england', 'runner_up_code': 'seattle',
     'winner_score': 28, 'runner_up_score': 24},
    {'code': '50', 'year': 2016, 'date': '2016-02-07', 'winner_code': 'denver', 'runner_up_code': 'carolina',
     'winner_score': 24, 'runner_up_score': 10},
    {'code': 'li', 'year': 2017, 'date': '2017-02-05', 'winner_code': 'new_england', 'runner_up_code': 'atlanta',
     'winner_score': 34, 'runner_up_score': 28},
    {'code': 'lii', 'year': 2018, 'date': '2018-02-04', 'winner_code': 'philadelphia',
     'runner_up_code': 'new_england', 'winner_score': 41, 'runner_up_score': 33},
    {'code': 'liii', 'year': 2019, 'date': '2019-02-03', 'winner_code': 'new_england',
     'runner_up_code': 'los_angeles_rams', 'winner_score': 13, 'runner_up_score': 3},
    {'code': 'liv', 'year': 2020, 'date': '2020-02-02', 'winner_code': 'kansas_city',
     'runner_up_code': 'san_francisco', 'winner_score': 31, 'runner_up_score': 20},
    {'code': 'lv', 'year': 2021, 'date': '2021-02-07', 'winner_code': 'tampa_bay', 'runner_up_code': 'kansas_city',
     'winner_score': 31, 'runner_up_score': 9},
    {'code': 'lvi', 'year': 2022, 'date': '2022-02-13', 'winner_code': 'los_angeles_rams',
     'runner_up_code': 'cincinnati', 'winner_score': 23, 'runner_up_score': 20}
]


@stored(key_config=FieldKeyConfig('code', FieldType.STR))
class SuperBowlResult:
    code: str = Attr()
    year: int = Attr(schema=ObjSchema(NumberSchema(minimum=1967), marshaller=NoOpMarshaller(int)))
    date: datetime
    winner_code: str
    runner_up_code: str
    winner_score: int
    runner_up_score: int


SUPER_BOWL_RESULTS = [SuperBowlResult(**{**r, 'date': datetime.fromisoformat(r['date'])})
                      for r in SUPER_BOWL_RESULT_DICTS]
