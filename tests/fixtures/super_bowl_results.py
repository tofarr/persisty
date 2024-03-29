from datetime import datetime

from schemey.schema import int_schema

from persisty.attr.attr import Attr
from persisty.attr.attr_type import AttrType
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.stored import stored

SUPER_BOWL_RESULT_DICTS = [
    {
        "code": "i",
        "result_year": 1967,
        "result_date": "1967-01-15T00:00:00+00:00",
        "winner_code": "green_bay",
        "runner_up_code": "kansas_city",
        "winner_score": 35,
        "runner_up_score": 10,
    },
    {
        "code": "ii",
        "result_year": 1968,
        "result_date": "1968-01-14T00:00:00+00:00",
        "winner_code": "green_bay",
        "runner_up_code": "oakland",
        "winner_score": 33,
        "runner_up_score": 14,
    },
    {
        "code": "iii",
        "result_year": 1969,
        "result_date": "1969-01-12T00:00:00+00:00",
        "winner_code": "new_york_jets",
        "runner_up_code": "baltimore",
        "winner_score": 16,
        "runner_up_score": 7,
    },
    {
        "code": "iv",
        "result_year": 1970,
        "result_date": "1970-01-11T00:00:00+00:00",
        "winner_code": "kansas_city",
        "runner_up_code": "minnesota",
        "winner_score": 23,
        "runner_up_score": 7,
    },
    {
        "code": "v",
        "result_year": 1971,
        "result_date": "1971-01-17T00:00:00+00:00",
        "winner_code": "baltimore",
        "runner_up_code": "dallas",
        "winner_score": 16,
        "runner_up_score": 13,
    },
    {
        "code": "vi",
        "result_year": 1972,
        "result_date": "1972-01-16T00:00:00+00:00",
        "winner_code": "dallas",
        "runner_up_code": "miami",
        "winner_score": 24,
        "runner_up_score": 3,
    },
    {
        "code": "vii",
        "result_year": 1973,
        "result_date": "1973-01-14T00:00:00+00:00",
        "winner_code": "miami",
        "runner_up_code": "washington",
        "winner_score": 14,
        "runner_up_score": 7,
    },
    {
        "code": "viii",
        "result_year": 1974,
        "result_date": "1974-01-13T00:00:00+00:00",
        "winner_code": "miami",
        "runner_up_code": "minnesota",
        "winner_score": 24,
        "runner_up_score": 7,
    },
    {
        "code": "ix",
        "result_year": 1975,
        "result_date": "1975-01-12T00:00:00+00:00",
        "winner_code": "pittsburgh",
        "runner_up_code": "minnesota",
        "winner_score": 16,
        "runner_up_score": 6,
    },
    {
        "code": "x",
        "result_year": 1976,
        "result_date": "1976-01-18T00:00:00+00:00",
        "winner_code": "pittsburgh",
        "runner_up_code": "dallas",
        "winner_score": 21,
        "runner_up_score": 17,
    },
    {
        "code": "xi",
        "result_year": 1977,
        "result_date": "1977-01-09T00:00:00+00:00",
        "winner_code": "oakland",
        "runner_up_code": "minnesota",
        "winner_score": 32,
        "runner_up_score": 14,
    },
    {
        "code": "xii",
        "result_year": 1978,
        "result_date": "1978-01-15T00:00:00+00:00",
        "winner_code": "dallas",
        "runner_up_code": "denver",
        "winner_score": 27,
        "runner_up_score": 10,
    },
    {
        "code": "xiii",
        "result_year": 1979,
        "result_date": "1979-01-21T00:00:00+00:00",
        "winner_code": "pittsburgh",
        "runner_up_code": "dallas",
        "winner_score": 35,
        "runner_up_score": 31,
    },
    {
        "code": "xiv",
        "result_year": 1980,
        "result_date": "1980-01-20T00:00:00+00:00",
        "winner_code": "pittsburgh",
        "runner_up_code": "los_angeles_rams",
        "winner_score": 31,
        "runner_up_score": 19,
    },
    {
        "code": "xv",
        "result_year": 1981,
        "result_date": "1981-01-25T00:00:00+00:00",
        "winner_code": "oakland",
        "runner_up_code": "philadelphia",
        "winner_score": 27,
        "runner_up_score": 10,
    },
    {
        "code": "xvi",
        "result_year": 1982,
        "result_date": "1982-01-24T00:00:00+00:00",
        "winner_code": "san_francisco",
        "runner_up_code": "cincinnati",
        "winner_score": 26,
        "runner_up_score": 21,
    },
    {
        "code": "xvii",
        "result_year": 1983,
        "result_date": "1983-01-30T00:00:00+00:00",
        "winner_code": "washington",
        "runner_up_code": "miami",
        "winner_score": 27,
        "runner_up_score": 17,
    },
    {
        "code": "xviii",
        "result_year": 1984,
        "result_date": "1984-01-22T00:00:00+00:00",
        "winner_code": "los_angeles_raiders",
        "runner_up_code": "washington",
        "winner_score": 38,
        "runner_up_score": 9,
    },
    {
        "code": "xix",
        "result_year": 1985,
        "result_date": "1985-01-20T00:00:00+00:00",
        "winner_code": "san_francisco",
        "runner_up_code": "miami",
        "winner_score": 38,
        "runner_up_score": 16,
    },
    {
        "code": "xx",
        "result_year": 1986,
        "result_date": "1986-01-26T00:00:00+00:00",
        "winner_code": "chicago",
        "runner_up_code": "new_england",
        "winner_score": 46,
        "runner_up_score": 10,
    },
    {
        "code": "xxi",
        "result_year": 1987,
        "result_date": "1987-01-25T00:00:00+00:00",
        "winner_code": "new_york_giants",
        "runner_up_code": "denver",
        "winner_score": 39,
        "runner_up_score": 20,
    },
    {
        "code": "xxii",
        "result_year": 1988,
        "result_date": "1988-01-31T00:00:00+00:00",
        "winner_code": "washington",
        "runner_up_code": "denver",
        "winner_score": 42,
        "runner_up_score": 10,
    },
    {
        "code": "xxiii",
        "result_year": 1989,
        "result_date": "1989-01-22T00:00:00+00:00",
        "winner_code": "san_francisco",
        "runner_up_code": "cincinnati",
        "winner_score": 20,
        "runner_up_score": 16,
    },
    {
        "code": "xxiv",
        "result_year": 1990,
        "result_date": "1990-01-28T00:00:00+00:00",
        "winner_code": "san_francisco",
        "runner_up_code": "denver",
        "winner_score": 55,
        "runner_up_score": 10,
    },
    {
        "code": "xxv",
        "result_year": 1991,
        "result_date": "1991-01-27T00:00:00+00:00",
        "winner_code": "new_york_giants",
        "runner_up_code": "buffalo",
        "winner_score": 20,
        "runner_up_score": 19,
    },
    {
        "code": "xxvi",
        "result_year": 1992,
        "result_date": "1992-01-26T00:00:00+00:00",
        "winner_code": "washington",
        "runner_up_code": "buffalo",
        "winner_score": 37,
        "runner_up_score": 24,
    },
    {
        "code": "xxvii",
        "result_year": 1993,
        "result_date": "1993-01-31T00:00:00+00:00",
        "winner_code": "dallas",
        "runner_up_code": "buffalo",
        "winner_score": 52,
        "runner_up_score": 17,
    },
    {
        "code": "xxviii",
        "result_year": 1994,
        "result_date": "1994-01-30T00:00:00+00:00",
        "winner_code": "dallas",
        "runner_up_code": "buffalo",
        "winner_score": 30,
        "runner_up_score": 13,
    },
    {
        "code": "xxix",
        "result_year": 1995,
        "result_date": "1995-01-29T00:00:00+00:00",
        "winner_code": "san_francisco",
        "runner_up_code": "san_diego",
        "winner_score": 49,
        "runner_up_score": 26,
    },
    {
        "code": "xxx",
        "result_year": 1996,
        "result_date": "1996-01-28T00:00:00+00:00",
        "winner_code": "dallas",
        "runner_up_code": "pittsburgh",
        "winner_score": 27,
        "runner_up_score": 17,
    },
    {
        "code": "xxxi",
        "result_year": 1997,
        "result_date": "1997-01-26T00:00:00+00:00",
        "winner_code": "green_bay",
        "runner_up_code": "new_england",
        "winner_score": 35,
        "runner_up_score": 21,
    },
    {
        "code": "xxxii",
        "result_year": 1998,
        "result_date": "1998-01-25T00:00:00+00:00",
        "winner_code": "denver",
        "runner_up_code": "green_bay",
        "winner_score": 31,
        "runner_up_score": 24,
    },
    {
        "code": "xxxiii",
        "result_year": 1999,
        "result_date": "1999-01-31T00:00:00+00:00",
        "winner_code": "denver",
        "runner_up_code": "atlanta",
        "winner_score": 34,
        "runner_up_score": 19,
    },
    {
        "code": "xxxiv",
        "result_year": 2000,
        "result_date": "2000-01-30T00:00:00+00:00",
        "winner_code": "st._louis",
        "runner_up_code": "tennessee",
        "winner_score": 23,
        "runner_up_score": 16,
    },
    {
        "code": "xxxv",
        "result_year": 2001,
        "result_date": "2001-01-28T00:00:00+00:00",
        "winner_code": "baltimore",
        "runner_up_code": "new_york_giants",
        "winner_score": 34,
        "runner_up_score": 7,
    },
    {
        "code": "xxxvi",
        "result_year": 2002,
        "result_date": "2002-02-03T00:00:00+00:00",
        "winner_code": "new_england",
        "runner_up_code": "st._louis",
        "winner_score": 20,
        "runner_up_score": 17,
    },
    {
        "code": "xxxvii",
        "result_year": 2003,
        "result_date": "2003-01-26T00:00:00+00:00",
        "winner_code": "tampa_bay",
        "runner_up_code": "oakland",
        "winner_score": 48,
        "runner_up_score": 21,
    },
    {
        "code": "xxxviii",
        "result_year": 2004,
        "result_date": "2004-02-01T00:00:00+00:00",
        "winner_code": "new_england",
        "runner_up_code": "carolina",
        "winner_score": 32,
        "runner_up_score": 29,
    },
    {
        "code": "xxxix",
        "result_year": 2005,
        "result_date": "2005-02-06T00:00:00+00:00",
        "winner_code": "new_england",
        "runner_up_code": "philadelphia",
        "winner_score": 24,
        "runner_up_score": 21,
    },
    {
        "code": "xl",
        "result_year": 2006,
        "result_date": "2006-02-05T00:00:00+00:00",
        "winner_code": "pittsburgh",
        "runner_up_code": "seattle",
        "winner_score": 21,
        "runner_up_score": 10,
    },
    {
        "code": "xli",
        "result_year": 2007,
        "result_date": "2007-02-04T00:00:00+00:00",
        "winner_code": "indianapolis",
        "runner_up_code": "chicago",
        "winner_score": 29,
        "runner_up_score": 17,
    },
    {
        "code": "xlii",
        "result_year": 2008,
        "result_date": "2008-02-03T00:00:00+00:00",
        "winner_code": "new_york_giants",
        "runner_up_code": "new_england",
        "winner_score": 17,
        "runner_up_score": 14,
    },
    {
        "code": "xliii",
        "result_year": 2009,
        "result_date": "2009-02-01T00:00:00+00:00",
        "winner_code": "pittsburgh",
        "runner_up_code": "arizona",
        "winner_score": 27,
        "runner_up_score": 23,
    },
    {
        "code": "xliv",
        "result_year": 2010,
        "result_date": "2010-02-07T00:00:00+00:00",
        "winner_code": "new_orleans",
        "runner_up_code": "indianapolis",
        "winner_score": 31,
        "runner_up_score": 17,
    },
    {
        "code": "xlv",
        "result_year": 2011,
        "result_date": "2011-02-06T00:00:00+00:00",
        "winner_code": "green_bay",
        "runner_up_code": "pittsburgh",
        "winner_score": 31,
        "runner_up_score": 25,
    },
    {
        "code": "xlvi",
        "result_year": 2012,
        "result_date": "2012-02-05T00:00:00+00:00",
        "winner_code": "new_york_giants",
        "runner_up_code": "new_england",
        "winner_score": 21,
        "runner_up_score": 17,
    },
    {
        "code": "xlvii",
        "result_year": 2013,
        "result_date": "2013-02-03T00:00:00+00:00",
        "winner_code": "baltimore",
        "runner_up_code": "san_francisco",
        "winner_score": 34,
        "runner_up_score": 31,
    },
    {
        "code": "xlviii",
        "result_year": 2014,
        "result_date": "2014-02-02T00:00:00+00:00",
        "winner_code": "seattle",
        "runner_up_code": "denver",
        "winner_score": 43,
        "runner_up_score": 8,
    },
    {
        "code": "xlix",
        "result_year": 2015,
        "result_date": "2015-02-01T00:00:00+00:00",
        "winner_code": "new_england",
        "runner_up_code": "seattle",
        "winner_score": 28,
        "runner_up_score": 24,
    },
    {
        "code": "50",
        "result_year": 2016,
        "result_date": "2016-02-07T00:00:00+00:00",
        "winner_code": "denver",
        "runner_up_code": "carolina",
        "winner_score": 24,
        "runner_up_score": 10,
    },
    {
        "code": "li",
        "result_year": 2017,
        "result_date": "2017-02-05T00:00:00+00:00",
        "winner_code": "new_england",
        "runner_up_code": "atlanta",
        "winner_score": 34,
        "runner_up_score": 28,
    },
    {
        "code": "lii",
        "result_year": 2018,
        "result_date": "2018-02-04T00:00:00+00:00",
        "winner_code": "philadelphia",
        "runner_up_code": "new_england",
        "winner_score": 41,
        "runner_up_score": 33,
    },
    {
        "code": "liii",
        "result_year": 2019,
        "result_date": "2019-02-03T00:00:00+00:00",
        "winner_code": "new_england",
        "runner_up_code": "los_angeles_rams",
        "winner_score": 13,
        "runner_up_score": 3,
    },
    {
        "code": "liv",
        "result_year": 2020,
        "result_date": "2020-02-02T00:00:00+00:00",
        "winner_code": "kansas_city",
        "runner_up_code": "san_francisco",
        "winner_score": 31,
        "runner_up_score": 20,
    },
    {
        "code": "lv",
        "result_year": 2021,
        "result_date": "2021-02-07T00:00:00+00:00",
        "winner_code": "tampa_bay",
        "runner_up_code": "kansas_city",
        "winner_score": 31,
        "runner_up_score": 9,
    },
    {
        "code": "lvi",
        "result_year": 2022,
        "result_date": "2022-02-13T00:00:00+00:00",
        "winner_code": "los_angeles_rams",
        "runner_up_code": "cincinnati",
        "winner_score": 23,
        "runner_up_score": 20,
    },
]


@stored(key_config=AttrKeyConfig("code", AttrType.STR))
class SuperBowlResult:
    code: str = Attr()
    result_year: int = Attr(schema=int_schema(minimum=1967), sortable=True)
    result_date: datetime
    winner_code: str
    runner_up_code: str
    winner_score: int
    runner_up_score: int


SUPER_BOWL_RESULTS = [
    SuperBowlResult(**{**r, "result_date": datetime.fromisoformat(r["result_date"])})
    for r in SUPER_BOWL_RESULT_DICTS
]
