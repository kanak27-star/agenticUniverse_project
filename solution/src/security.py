
import hashlib
import hmac
from pathlib import Path

from cryptography.fernet import Fernet


class SecurityManager:
    """
    Handles encryption, decryption, and secure phone-number lookup.

    - Fernet encrypts sensitive information.
    - HMAC-SHA256 creates deterministic phone lookup hashes.
    - The encryption key is saved locally and reused across runs.
    """

    def __init__(self, key_path: str | Path | None = None):
        """
        Initialize the security manager.

        If no key path is provided, the key is stored at:
        solution/secrets/master.key
        """

        if key_path is None:
            project_root = Path(__file__).resolve().parents[2]

            key_path = (
                project_root
                / "solution"
                / "secrets"
                / "master.key"
            )

        self.key_path = Path(key_path)

        # Create the secrets directory if it does not exist.
        self.key_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        # Reuse the existing key or generate a new one.
        if self.key_path.exists():
            key = self.key_path.read_bytes().strip()
        else:
            key = Fernet.generate_key()

            self.key_path.write_bytes(key)

            # Restrict file permissions where supported.
            try:
                self.key_path.chmod(0o600)
            except OSError:
                pass

        # Initialize Fernet encryption.
        self.cipher = Fernet(key)

        # Derive a separate key for phone lookup hashes.
        self.lookup_key = hmac.new(
            key,
            b"agentic-universe-phone-lookup-v1",
            hashlib.sha256
        ).digest()

    def encrypt(self, value: str | None) -> str | None:
        """
        Encrypt a string value.

        Returns None if the input is None.
        """

        if value is None:
            return None

        encrypted_value = self.cipher.encrypt(
            value.encode("utf-8")
        )

        return encrypted_value.decode("utf-8")

    def decrypt(self, encrypted_value: str | None) -> str | None:
        """
        Decrypt a previously encrypted string.

        Returns None if the input is None.
        """

        if encrypted_value is None:
            return None

        decrypted_value = self.cipher.decrypt(
            encrypted_value.encode("utf-8")
        )

        return decrypted_value.decode("utf-8")

    def phone_lookup_hash(self, canonical_phone: str) -> str:
        """
        Generate a deterministic HMAC-SHA256 hash.

        The same canonical phone number always produces
        the same hash when the same encryption key is used.

        This allows duplicate phone lookup without storing
        the original phone number in plain text.
        """

        return hmac.new(
            self.lookup_key,
            canonical_phone.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
