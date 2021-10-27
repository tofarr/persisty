from persisty import PersistyContext
from persisty.store.store_abc import StoreABC
from tests.fixtures.items import Band, Member

BANDS = [
    Band('beatles', 'The Beatles', 1960),
    Band('rolling_stones', 'The Rolling Stones', 1962),
    Band('led_zeppelin', 'Led Zeppelin', 1968)
]

MEMBERS = [
    Member('john', 'John Lennon', 'beatles', '1940-10-09'),
    Member('paul', 'Paul McCartney', 'beatles', '1942-06-18'),
    Member('george', 'George Harrison', 'beatles', '1943-02-25'),
    Member('ringo', 'Ringo Starr', 'beatles', '1940-07-07'),
    Member('jagger', 'Mick Jagger', 'rolling_stones', '1943-07-26'),
    Member('brian_jones', 'Brian Jones', 'rolling_stones', '1942-02-28'),
    Member('richards', 'Kieth Richards', 'rolling_stones', '1943-12-18'),
    Member('wyman', 'Bill Wyman', 'rolling_stones', '1936-10-24'),
    Member('watts', 'Charlie Watts', 'rolling_stones', '1941-06-02'),
    Member('plant', 'Robert Plant', 'led_zeppelin', '1948-08-20'),
    Member('page', 'Jimmy Page', 'led_zeppelin', '1944-01-09'),
    Member('john_paul_jones', 'John Paul Jones', 'led_zeppelin', '1946-01-03'),
    Member('bonham', 'John Bonham', 'led_zeppelin', '1940-05-31')
]


def setup_test_data(persisty_context: PersistyContext):
    setup_bands(persisty_context.get_store(Band))
    setup_members(persisty_context.get_store(Member))


def setup_bands(store: StoreABC[Band]):
    for band in BANDS:
        store.create(band)


def setup_members(store: StoreABC[Member]):
    for member in MEMBERS:
        store.create(member)
