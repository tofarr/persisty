if __name__ == '__main__':
    bands = [
        BandEntity('beatles', 'The Beatles'),
        BandEntity('rolling_stones', 'The Rolling Stones'),
        BandEntity('led_zeppelin', 'Led Zeppelin')
    ]
    for band in bands:
        band.save()

    members = [
        MemberEntity('john', 'John Lennon', 'beatles'),
        MemberEntity('paul', 'Paul McCartney', 'beatles'),
        MemberEntity('george', 'George Harrison', 'beatles'),
        MemberEntity('ringo', 'Ringo Starr', 'beatles'),
        MemberEntity('jagger', 'Mick Jagger', 'rolling_stones'),
        MemberEntity('jones', 'Brian Jones', 'rolling_stones'),
        MemberEntity('richards', 'Kieth Richards', 'rolling_stones'),
        MemberEntity('wyman', 'Bill Wyman', 'rolling_stones'),
        MemberEntity('watts', 'Charlie Watts', 'rolling_stones'),
        MemberEntity('plant', 'Robert Plant', 'led_zeppelin'),
        MemberEntity('page', 'Jimmy Page', 'led_zeppelin'),
        MemberEntity('jones', 'John Paul Jones', 'led_zeppelin'),
        MemberEntity('bonham', 'John Bonham', 'led_zeppelin')
    ]
    for member in members:
        member.save()

    member = MemberEntity.read('john')
    print(member)
    print(member.band_id)
    print(member.member_name)
    print(member.band)
    print(member.band.members)

    for band in BandEntity.search():
        print(band)
        print(band.members)
