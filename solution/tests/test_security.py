
from solution.src.security import SecurityManager


def test_encrypt_and_decrypt():
    security = SecurityManager()

    original = "9876543210"
    encrypted = security.encrypt(original)

    assert encrypted != original
    assert security.decrypt(encrypted) == original


def test_same_phone_has_same_lookup_hash():
    security = SecurityManager()

    first_hash = security.phone_lookup_hash("+919876543210")
    second_hash = security.phone_lookup_hash("+919876543210")

    assert first_hash == second_hash


def test_different_phones_have_different_hashes():
    security = SecurityManager()

    first_hash = security.phone_lookup_hash("+919876543210")
    second_hash = security.phone_lookup_hash("+918765432109")

    assert first_hash != second_hash


def test_none_values():
    security = SecurityManager()

    assert security.encrypt(None) is None
    assert security.decrypt(None) is None
