from persisty.storage.storage_context import StorageContext
from tests.fixtures.item_types import Band, Member, Tag, Node, NodeTag

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

important = Tag('Important')
spam = Tag('Spam')
TAGS = [important, spam]

root = Node('Root')
child_a = Node('Child A', parent_id=root.id)
child_b = Node('Child B', parent_id=root.id)
grandchild_a_a = Node('Grandchild A-A', parent_id=child_a.id)
grandchild_a_b = Node('Grandchild A-B', parent_id=child_a.id)
NODES = [root, child_a, child_b, grandchild_a_a, grandchild_a_b]

NODE_TAGS = [
    NodeTag(child_b.id, spam.id),
    NodeTag(child_a.id, important.id)
]


def populate_data(context: StorageContext):
    for band in BANDS:
        context.get_storage(Band).create(band)
    for member in MEMBERS:
        context.get_storage(Member).create(member)
    for tag in TAGS:
        context.get_storage(Tag).create(tag)
    for node in NODES:
        context.get_storage(Node).create(node)
    for node_tag in NODE_TAGS:
        context.get_storage(NodeTag).create(node_tag)
