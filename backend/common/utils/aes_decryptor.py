from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex


class Prpcrypt:
    def __init__(self):
        self.key = "eCcGFZQj6PNoSSma31LR39rTzTbLkU8E".encode("utf-8")
        self.mode = AES.MODE_CBC

    # Encrypt text. If text length is less than 16, pad to 16.
    # If text length is greater than 16 and not a multiple of 16, pad to next multiple.
    def encrypt(self, text):
        cryptor = AES.new(self.key, self.mode, b"0000000000000000")
        # Key length must be 16 (AES-128), 24 (AES-192), or 32 (AES-256) bytes.
        # AES-128 is sufficient for current usage.
        length = 16
        count = len(text)
        if count < length:
            add = length - count
            # \0 padding
            text = text + ("\0" * add)
        elif count > length:
            add = length - (count % length)
            text = text + ("\0" * add)
        self.ciphertext = cryptor.encrypt(text.encode("utf-8"))
        # Encrypted bytes are not guaranteed to be ASCII-safe.
        # Convert ciphertext to hexadecimal string for stable output/storage.
        return b2a_hex(self.ciphertext).decode(encoding="utf-8")

    # Decrypt and remove trailing padding characters.
    def decrypt(self, text):
        cryptor = AES.new(self.key, self.mode, b"0000000000000000")
        plain_text = cryptor.decrypt(a2b_hex(text))
        return plain_text.decode().rstrip("\0")


if __name__ == "__main__":
    pc = Prpcrypt()  # Initialize key
    e = pc.encrypt("123456")  # Encrypt
    d = pc.decrypt(e)  # Decrypt
    print("Encrypted:", str(e))
    print("Decrypted:", str(d))
