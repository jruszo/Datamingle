LEGACY_MIRAGE_SECRET_KEY = "test-mirage-secret-key-1234567890"
LEGACY_MIRAGE_CBC_IV = "fedcba0987654321"

# These ciphertexts were generated from the pre-migration Mirage AES/ECB format
# with LEGACY_MIRAGE_SECRET_KEY and are pinned here to keep tests deterministic.
LEGACY_MIRAGE_CIPHERTEXTS = {
    "legacy-user": "_Z3ltHqLM3ElYkNjsFGCMA==",
    "legacy-root": "MVDwR1ka9b_2PNsblc75QQ==",
    "legacy-password": "whY6rTtCeZicLTJmkbkXhg==",
    "legacy-ak": "lqN38b1I1guLDVHUFmQZ4A==",
    "legacy-sk": "xDwJ96BefaSfmGXSH_mThQ==",
}

# This ciphertext was generated from the pre-migration Mirage AES/CBC format
# with LEGACY_MIRAGE_SECRET_KEY and LEGACY_MIRAGE_CBC_IV.
LEGACY_MIRAGE_CBC_CIPHERTEXTS = {
    "legacy-cbc-user": "DvylVMJdhBSwATSUJMZiOg==",
}
