from __future__ import annotations

import hashlib
import logging
import os
from time import time

logger = logging.getLogger(__name__)


class TikTokSigner:
    """
    X-Bogus and X-Gnarly Generation Algorithms for TikTok API Authentication.
    """

    shift_array = "Dkdpgh4ZKsQB80/Mfvw36XI1R25-WUAlEi7NLboqYTOPuzmFjJnryx9HVGcaStCe"
    magic = 536919696

    @staticmethod
    def md5_2x(string: str) -> str:
        return hashlib.md5(hashlib.md5(string.encode()).digest()).hexdigest()

    @staticmethod
    def rc4_encrypt(plaintext: str, key: list[int]) -> str:
        s_box = list(range(256))
        index = 0
        for _ in range(256):
            index = (index + s_box[_] + key[_ % len(key)]) % 256
            s_box[_], s_box[index] = s_box[index], s_box[_]
        _ = 0
        index = 0
        ciphertext = ""
        for char in plaintext:
            _ = (_ + 1) % 256
            index = (index + s_box[_]) % 256
            s_box[_], s_box[index] = s_box[index], s_box[_]
            keystream = s_box[(s_box[_] + s_box[index]) % 256]
            ciphertext += chr(ord(char) ^ keystream)
        return ciphertext

    @staticmethod
    def b64_encode(
        string: str,
        key_table: str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=",
    ) -> str:
        last_list = []
        for i in range(0, len(string), 3):
            try:
                num_1 = ord(string[i])
                num_2 = ord(string[i + 1])
                num_3 = ord(string[i + 2])
                arr_1 = num_1 >> 2
                arr_2 = (3 & num_1) << 4 | (num_2 >> 4)
                arr_3 = ((15 & num_2) << 2) | (num_3 >> 6)
                arr_4 = 63 & num_3
            except IndexError:
                arr_1 = num_1 >> 2
                arr_2 = ((3 & num_1) << 4) | 0
                arr_3 = 64
                arr_4 = 64
            last_list.extend([arr_1, arr_2, arr_3, arr_4])
        return "".join([key_table[value] for value in last_list])

    @staticmethod
    def filter(num_list: list) -> list:
        indices = [3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 4, 6, 8, 10, 12, 14, 16, 18, 20]
        return [num_list[x - 1] for x in indices]

    @staticmethod
    def scramble(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) -> str:  # noqa: E741
        return "".join([chr(_) for _ in [a, k, b, l, c, m, d, n, e, o, f, p, g, q, h, r, i, s, j]])

    @staticmethod
    def checksum(salt_list: list) -> int:
        chk = 64
        for x in salt_list[3:]:
            chk ^= x
        return chk

    @classmethod
    def generate_x_bogus(
        cls, query_string: str, user_agent: str, timestamp: int = None, data: str = ""
    ) -> str:
        if timestamp is None:
            timestamp = int(time())
        md5_data = cls.md5_2x(data)
        md5_params = cls.md5_2x(query_string)
        md5_ua = hashlib.md5(
            cls.b64_encode(cls.rc4_encrypt(user_agent, [0, 1, 14])).encode()
        ).hexdigest()
        salt_list = [
            timestamp,
            cls.magic,
            64,
            0,
            1,
            14,
            bytes.fromhex(md5_params)[-2],
            bytes.fromhex(md5_params)[-1],
            bytes.fromhex(md5_data)[-2],
            bytes.fromhex(md5_data)[-1],
            bytes.fromhex(md5_ua)[-2],
            bytes.fromhex(md5_ua)[-1],
        ]
        salt_list.extend([(timestamp >> i) & 0xFF for i in range(24, -1, -8)])
        salt_list.extend([(salt_list[1] >> i) & 0xFF for i in range(24, -1, -8)])
        salt_list.extend([cls.checksum(salt_list), 255])
        num_list = cls.filter(salt_list)
        rc4_num_list = cls.rc4_encrypt(cls.scramble(*num_list), [255])
        return cls.b64_encode(f"\x02ÿ{rc4_num_list}", cls.shift_array)

    @classmethod
    def generate_x_gnarly(
        cls, query_string: str, user_agent: str, body: str = "", timestamp: int = None
    ) -> str:
        # Direct port of xgnarly.mjs from tiktok-signature v4.3.6
        SIGMA = [1196819126, 600974999, 3863347763, 1451689750]
        ALPHABET = "u09tbS3UvgDEe6r-ZVMXzLpsAohTn7mdINQlW412GqBjfYiyk8JORCF5/xKHwacP="
        MAGIC_BYTE = 75
        FIELD_ORDER = [1, 8, 12, 11, 6, 9, 4, 7, 0, 14, 15, 2, 3, 10, 5, 13]
        INT_WIDTHS = {0: 4, 1: 2, 2: 2, 6: 4, 7: 4, 8: 4, 11: 2, 12: 2, 13: 2, 14: 4, 15: 4}

        ts_ms = int(time() * 1000) if timestamp is None else int(timestamp) * 1000
        ts_sec = ts_ms // 1000

        r14 = os.urandom(2)
        r15 = os.urandom(4)
        field14 = (65 << 16) | (r14[0] << 8) | r14[1]
        field15 = ((r15[0] << 24) | (r15[1] << 16) | (r15[2] << 8) | r15[3]) & 0xFFFFFFFF

        fields: dict = {
            1: 65,
            2: 4,
            3: hashlib.md5(query_string.encode()).hexdigest(),
            4: hashlib.md5(body.encode()).hexdigest(),
            5: hashlib.md5(user_agent.encode()).hexdigest(),
            6: ts_sec,
            7: 3181061566,
            8: ts_ms % 0x80000000,
            9: "5.1.3-ZTCA",
            10: "1.0.0.368",
            11: 1,
            12: 0,
            13: 0,
            14: field14,
            15: field15,
        }

        xor_header = 0
        for v in fields.values():
            if isinstance(v, int):
                xor_header = (xor_header ^ v) & 0xFFFFFFFF
        fields[0] = xor_header

        def _int_to_be(n: int, width: int) -> list:
            return [(n >> (8 * (width - 1 - i))) & 0xFF for i in range(width)]

        present = [k for k in FIELD_ORDER if k in fields]
        payload: list = [len(present)]
        for k in present:
            v = fields[k]
            vbytes = _int_to_be(v, INT_WIDTHS[k]) if isinstance(v, int) else list(v.encode())
            vlen = len(vbytes)
            payload.append(k & 0xFF)
            payload.extend([(vlen >> 8) & 0xFF, vlen & 0xFF])
            payload.extend(vbytes)
        plaintext = bytearray(payload)

        key_bytes = os.urandom(48)
        key_words = [
            (key_bytes[i * 4] | (key_bytes[i * 4 + 1] << 8) | (key_bytes[i * 4 + 2] << 16) | (key_bytes[i * 4 + 3] << 24)) & 0xFFFFFFFF
            for i in range(12)
        ]
        rounds = (sum(w & 15 for w in key_words) & 15) + 5

        def _u32(x: int) -> int:
            return x & 0xFFFFFFFF

        def _rotl(v: int, c: int) -> int:
            return _u32((v << c) | (v >> (32 - c)))

        def _quarter(s: list, a: int, b: int, c: int, d: int) -> None:
            s[a] = _u32(s[a] + s[b]); s[d] = _rotl(s[d] ^ s[a], 16)
            s[c] = _u32(s[c] + s[d]); s[b] = _rotl(s[b] ^ s[c], 12)
            s[a] = _u32(s[a] + s[b]); s[d] = _rotl(s[d] ^ s[a], 8)
            s[c] = _u32(s[c] + s[d]); s[b] = _rotl(s[b] ^ s[c], 7)

        def _chacha_block(initial: list, rds: int) -> list:
            s = list(initial)
            r = 0
            while r < rds:
                _quarter(s, 0, 4, 8, 12); _quarter(s, 1, 5, 9, 13)
                _quarter(s, 2, 6, 10, 14); _quarter(s, 3, 7, 11, 15)
                r += 1
                if r >= rds:
                    break
                _quarter(s, 0, 5, 10, 15); _quarter(s, 1, 6, 11, 12)
                _quarter(s, 2, 7, 12, 13); _quarter(s, 3, 4, 13, 14)
                r += 1
            for i in range(16):
                s[i] = _u32(s[i] + initial[i])
            return s

        state = list(SIGMA) + key_words
        cipher = bytearray(plaintext)
        for off in range(0, len(cipher), 64):
            stream = _chacha_block(state, rounds)
            state[12] = _u32(state[12] + 1)
            lim = min(64, len(cipher) - off)
            for i in range(lim):
                cipher[off + i] ^= (stream[i >> 2] >> (8 * (i & 3))) & 0xFF

        mod = len(cipher) + 1
        s = 0
        for b in key_bytes:
            s = (s + b) % mod
        for b in cipher:
            s = (s + b) % mod
        insert_pos = s

        raw = bytes([MAGIC_BYTE]) + bytes(cipher[:insert_pos]) + key_bytes + bytes(cipher[insert_pos:])

        out = []
        i = 0
        while i + 3 <= len(raw):
            n = (raw[i] << 16) | (raw[i + 1] << 8) | raw[i + 2]
            out.append(ALPHABET[(n >> 18) & 63])
            out.append(ALPHABET[(n >> 12) & 63])
            out.append(ALPHABET[(n >> 6) & 63])
            out.append(ALPHABET[n & 63])
            i += 3
        rem = len(raw) - i
        if rem == 1:
            n = raw[i] << 16
            out.append(ALPHABET[(n >> 18) & 63])
            out.append(ALPHABET[(n >> 12) & 63])
            out.append("=")
            out.append("=")
        elif rem == 2:
            n = (raw[i] << 16) | (raw[i + 1] << 8)
            out.append(ALPHABET[(n >> 18) & 63])
            out.append(ALPHABET[(n >> 12) & 63])
            out.append(ALPHABET[(n >> 6) & 63])
            out.append("=")
        return "".join(out)