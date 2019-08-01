"""
Variables that influence testing behavior are defined here.
"""
import string
from random import choice as random_choise

from nacl.encoding import URLSafeBase64Encoder as encoder
from nacl.public import Box, PrivateKey
from nacl.utils import random
from pgpy import PGPUID, PGPKey
from pgpy.constants import (CompressionAlgorithm, HashAlgorithm, KeyFlags, PubKeyAlgorithm, SymmetricKeyAlgorithm)


def gen_keypair():
    """
    Generate new nacl key pair
    """
    pair = PrivateKey.generate()
    secret_str = pair.encode(encoder).decode('utf-8')
    public_str = pair.public_key.encode(encoder).decode('utf-8')
    keys = {
        'secret_obj': pair,
        'secret': secret_str,
        'public_obj': pair.public_key,
        'public': public_str,
    }
    return keys


def gen_plain_random_string(length=64):
    """
    Generate random alphanum sequence
    """
    return ''.join(random_choise(string.ascii_letters + string.digits) for _ in range(length))


def gen_gpg(name, email='test@test.com'):
    """
    Create new PGP key
    """
    key = PGPKey.new(PubKeyAlgorithm.RSAEncryptOrSign, 4096)
    uid = PGPUID.new(name, comment=name, email=email)

    key.add_uid(
        uid,
        usage={
            KeyFlags.Sign,
            KeyFlags.EncryptCommunications,
            KeyFlags.EncryptStorage,
        },
        hashes=[
            HashAlgorithm.SHA256,
            HashAlgorithm.SHA384,
            HashAlgorithm.SHA512,
            HashAlgorithm.SHA224,
        ],
        ciphers=[
            SymmetricKeyAlgorithm.AES256,
            SymmetricKeyAlgorithm.AES192,
            SymmetricKeyAlgorithm.AES128,
        ],
        compression=[
            CompressionAlgorithm.ZLIB,
            CompressionAlgorithm.BZ2,
            CompressionAlgorithm.ZIP,
            CompressionAlgorithm.Uncompressed,
        ])

    return str(key), key.fingerprint.keyid


class CryptoBox(Box):
    """
    Subclass Box to avoid boilerplating in encryption.
    Usually we just need an encrypted, encoded string,
    which is provided by the following method.
    """

    def encrypt_utf(self, data, *args, **kwargs):
        """
        Return UTF-8 string, using explicit nonce
        """
        assert isinstance(data, str), 'data argument must be a string'

        # This is a nonce, it *MUST* only be used once,
        # but it is not considered
        # secret and can be transmitted or stored alongside the ciphertext. A
        # good source of nonces are just sequences of 24 random bytes.
        nonce = random(self.NONCE_SIZE)
        # Raw encrypted byte sequence.
        raw = self.encrypt(data.encode('utf8'), nonce, *args, **kwargs)
        # Decode it to printable string.
        return encoder.encode(raw).decode('utf8')
