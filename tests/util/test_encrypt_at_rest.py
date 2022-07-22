from unittest import TestCase

from persisty.util.encrypt_at_rest import encrypt, decrypt


class TestEncryptAtRest(TestCase):
    def test_round_trip(self):
        msg = "This is a test"
        encrypted = encrypt(msg)
        self.assertNotEqual(encrypted, msg)
        decrypted = decrypt(encrypted)
        self.assertEqual(decrypted, msg)

    def test_invalid(self):
        encrypted = None
        # noinspection PyBroadException
        try:
            msg = "This is a test"
            encrypted = decrypt(msg)
        except:
            pass
        self.assertIsNone(encrypted)
