import base64
import hashlib
import hmac
import secrets
from typing import AnyStr, Tuple, TypeVar

import orjson
from Crypto.Cipher import AES
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, utils
from google.cloud.kms import KeyManagementServiceAsyncClient

kek_metadata_key_name = "key"
dek_metadata_key_name = "dek"
nonce_metadata_key_name = "nonce"
hash_metadata_key_name = "hash"
sig_metadata_key_name = "sig"
sig_key_metadata_key_name = "sigKey"

metadata_required_keys = frozenset(
    [
        kek_metadata_key_name,
        dek_metadata_key_name,
        nonce_metadata_key_name,
        hash_metadata_key_name,
        sig_metadata_key_name,
        sig_key_metadata_key_name,
    ]
)
metadata_signature_keys = frozenset(
    [
        kek_metadata_key_name,
        dek_metadata_key_name,
        nonce_metadata_key_name,
        hash_metadata_key_name,
        sig_key_metadata_key_name,
    ]
)

dek_byte_length = 32
auth_tag_byte_length = 16  # This is for compatibility with Go's `crypto.cipher.AEAD`
nonce_byte_length = 12  # This is for compatibility with Go's `crypto.cipher.AEAD`

_T = TypeVar("_T")


class Cryptographer:
    """A client encrypting and decrypting data with Envelope Encryption via Google KMS."""

    __slots__ = ("kms",)

    def __init__(self, dev: bool = False):
        self.kms = KeyManagementServiceAsyncClient()

    async def decrypt(self, ciphertext: AnyStr, metadata: dict):
        # Get the object's metadata. This should contain the required encryption keys.
        kek_name, encrypted_dek, nonce, hashcode = await self._validate_metadata(
            metadata
        )
        # Decrypt the data with the keys extracted from the metadata.
        cleartext = await self._decrypt(kek_name, encrypted_dek, nonce, ciphertext)
        # Get a hashcode of the given data and ensure it matches the expected hash.
        computed_hashcode = hashlib.sha256(cleartext).digest()
        if not hmac.compare_digest(hashcode, computed_hashcode):
            raise ValueError(
                f"data hashcode comparison failed: "
                f"expected {hashcode} but found {computed_hashcode}"
            )
        return cleartext, metadata

    async def encrypt(
        self,
        cleartext: AnyStr,
        kek_name: str,
        signing_key_name: str,
    ) -> Tuple[str, bytes, dict]:
        """Encrypt `cleartext` using the defined KEK provided by `kek_name` and upload it.

        Args:
            cleartext: The data to encrypt and upload.
            kek_name: The name of the target KEK in GC KMS.
            signing_key_name: The name used to determine the signing key for encryption.

        Returns:
            hashcode: A sha256 fingerprint for this data.
            ciphertext: The encrypted data.
            metadata: Encryption metadata required for decryption.
        """
        if isinstance(cleartext, str):
            cleartext = cleartext.encode("utf8")
        # Encrypt the data
        ciphertext, encrypted_dek, nonce = await self._encrypt(kek_name, cleartext)
        # Build the metadata for this object.
        # N.B. - This rstrip for compatibility with `base64.RawStdEncoding` in Go
        #   which uses no pad.
        unpadded_dek = base64.b64encode(encrypted_dek).rstrip(b"=").decode()
        unpadded_nonce = base64.b64encode(nonce).rstrip(b"=").decode()
        hashcode = hashlib.sha256(cleartext).hexdigest()
        metadata = {
            kek_metadata_key_name: kek_name,
            dek_metadata_key_name: unpadded_dek,
            nonce_metadata_key_name: unpadded_nonce,
            hash_metadata_key_name: hashcode,
            sig_key_metadata_key_name: signing_key_name,
        }
        # Get the signing key for this object.
        signature = await self._sign_metadata(signing_key_name, metadata)
        # N.B. - This rstrip for compatibility with `base64.RawStdEncoding` in Go
        #   which uses no pad.
        unpadded_signature = base64.b64encode(signature).rstrip(b"=").decode()
        metadata[sig_metadata_key_name] = unpadded_signature
        return hashcode, ciphertext, metadata

    @staticmethod
    def _fingerprint_metadata(metadata: dict) -> bytes:
        sanitized = {
            key: metadata[key] for key in metadata_signature_keys & metadata.keys()
        }
        # N.B. - This is for compatibility with `json.Marshal` in Go
        #   which sorts keys and uses no whitespace.
        encoded = orjson.dumps(sanitized, option=orjson.OPT_SORT_KEYS)
        return hashlib.sha256(encoded).digest()

    async def _sign_metadata(self, key_name: str, metadata: dict) -> bytes:
        fingerprint = self._fingerprint_metadata(metadata)
        request = {"name": key_name, "digest": {"sha256": fingerprint}}
        response = await self.kms.asymmetric_sign(request=request)

        return response.signature

    async def _encrypt(
        self, kek_name: str, cleartext: AnyStr
    ) -> [Tuple[bytes, bytes, bytes]]:
        nonce = secrets.token_bytes(nonce_byte_length)
        dek = secrets.token_bytes(dek_byte_length)

        cipher = AES.new(dek, AES.MODE_GCM, nonce=nonce, mac_len=auth_tag_byte_length)
        ciphertext, tag = cipher.encrypt_and_digest(cleartext)
        # N.B. - This is for compatibility with AES-GCM in Go
        #   which automatically appends the MAC tag to the end of the ciphertext.
        ciphertext += tag
        response = await self.kms.encrypt(request={"name": kek_name, "plaintext": dek})
        encrypted_dek = response.ciphertext

        return ciphertext, encrypted_dek, nonce

    @staticmethod
    def _pad_encoded_value(value):
        if len(value) % 4:
            value += "=" * (4 - (len(value) % 4))

        return value

    async def _validate_metadata(
        self, metadata: dict
    ) -> Tuple[str, bytes, bytes, bytes]:
        for key in metadata_required_keys:
            if key not in metadata:
                raise ValueError(f"Missing metadata key: {key}")

        sig_key_name = metadata[sig_key_metadata_key_name]
        encoded_signature = self._pad_encoded_value(metadata[sig_metadata_key_name])
        signature = base64.b64decode(encoded_signature)

        public_key = await self.kms.get_public_key(request={"name": sig_key_name})
        pem = public_key.pem.encode("utf-8")
        ec_key = serialization.load_pem_public_key(pem, default_backend())

        fingerprint = self._fingerprint_metadata(metadata)

        sha256 = hashes.SHA256()
        ec_key.verify(signature, fingerprint, ec.ECDSA(utils.Prehashed(sha256)))

        encoded_nonce = self._pad_encoded_value(metadata[nonce_metadata_key_name])
        nonce = base64.b64decode(encoded_nonce)

        encoded_dek = self._pad_encoded_value(metadata[dek_metadata_key_name])
        encrypted_dek = base64.b64decode(encoded_dek)

        hashcode = bytes.fromhex(metadata[hash_metadata_key_name])

        return metadata[kek_metadata_key_name], encrypted_dek, nonce, hashcode

    async def _decrypt(self, key_name, encrypted_dek, nonce, ciphertext_and_tag):
        response = await self.kms.decrypt(
            request={"name": key_name, "ciphertext": encrypted_dek}
        )

        auth_tag = ciphertext_and_tag[len(ciphertext_and_tag) - auth_tag_byte_length :]
        ciphertext = ciphertext_and_tag[
            : len(ciphertext_and_tag) - auth_tag_byte_length
        ]
        dek = response.plaintext
        cipher = AES.new(dek, AES.MODE_GCM, nonce=nonce, mac_len=auth_tag_byte_length)
        return cipher.decrypt_and_verify(ciphertext, auth_tag)
