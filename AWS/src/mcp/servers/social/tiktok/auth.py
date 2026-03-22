from __future__ import annotations
import logging
import hashlib
import math
import random
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
        s_box = [_ for _ in range(256)]
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
    def b64_encode(string: str, key_table: str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=") -> str:
        last_list = list()
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
    def scramble(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) -> str:
        return "".join([chr(_) for _ in [a, k, b, l, c, m, d, n, e, o, f, p, g, q, h, r, i, s, j]])

    @staticmethod
    def checksum(salt_list: list) -> int:
        chk = 64
        for x in salt_list[3:]:
            chk ^= x
        return chk

    @classmethod
    def generate_x_bogus(cls, query_string: str, user_agent: str, timestamp: int = None, data: str = "") -> str:
        if timestamp is None:
            timestamp = int(time())
        md5_data = cls.md5_2x(data)
        md5_params = cls.md5_2x(query_string)
        md5_ua = hashlib.md5(cls.b64_encode(cls.rc4_encrypt(user_agent, [0, 1, 14])).encode()).hexdigest()
        salt_list = [timestamp, cls.magic, 64, 0, 1, 14, bytes.fromhex(md5_params)[-2], bytes.fromhex(md5_params)[-1], bytes.fromhex(md5_data)[-2], bytes.fromhex(md5_data)[-1], bytes.fromhex(md5_ua)[-2], bytes.fromhex(md5_ua)[-1]]
        salt_list.extend([(timestamp >> i) & 0xFF for i in range(24, -1, -8)])
        salt_list.extend([(salt_list[1] >> i) & 0xFF for i in range(24, -1, -8)])
        salt_list.extend([cls.checksum(salt_list), 255])
        num_list = cls.filter(salt_list)
        rc4_num_list = cls.rc4_encrypt(cls.scramble(*num_list), [255])
        return cls.b64_encode(f"\x02ÿ{rc4_num_list}", cls.shift_array)

    @classmethod
    def generate_x_gnarly(cls, query_string: str, user_agent: str, body: str = "", timestamp: int = None) -> str:
        obj = {1: 1, 2: 0, 3: hashlib.md5(query_string.encode('utf-8')).hexdigest(), 4: hashlib.md5(body.encode('utf-8')).hexdigest(), 5: hashlib.md5(user_agent.encode('utf-8')).hexdigest()}
        t_ms = int(time() * 1000) if timestamp is None else timestamp * 1000
        obj[6], obj[7], obj[8], obj[9] = t_ms // 1000, 1245783967, t_ms % 2147483648, "5.1.0"
        obj[0] = obj[6] ^ obj[7] ^ obj[8] ^ obj[1] ^ obj[2]
        arr = [len(obj)]
        def n2u(v): return list(v.to_bytes(2, 'big')) if v < 65025 else list(v.to_bytes(4, 'big'))
        for k in sorted(obj.keys()):
            v = obj[k]
            arr.append(int(k))
            va = n2u(v) if isinstance(v, int) else list(v.encode('utf-8'))
            arr.extend(n2u(len(va)))
            arr.extend(va)
        s_arr = "".join(chr(c) for c in arr)
        someRandomChar = chr(( (1 << 6) ^ (1 << 3) ^ 3 ) & 255)
        key, keyStringArr, rounds, l_St, l_kt = [], [], 0, 0, [44, 28, 3212677781, 1, 217618912, 2931180889, 1498001188, 2157053261, 211147047, 185100057, 2903579748, 3732962506, 4294967295 & (t_ms // 1000), int(4294967296 * 0.5), int(4294967296 * 0.5), int(4294967296 * 0.5)]
        def Ab33(e, t):
            r = list(e)
            def Ab41(e, t): return ((e << t) & 0xFFFFFFFF) | (e >> (32 - t))
            def Ab18(e, t, r, n, o):
                e[t] = (e[t] + e[r]) & 0xFFFFFFFF
                e[o], e[n] = Ab41(e[o] ^ e[t], 16), (e[n] + e[o]) & 0xFFFFFFFF
                e[r], e[t] = Ab41(e[r] ^ e[n], 12), (e[t] + e[r]) & 0xFFFFFFFF
                e[o], e[n] = Ab41(e[o] ^ e[t], 8), (e[n] + e[o]) & 0xFFFFFFFF
                e[r] = Ab41(e[r] ^ e[n], 7)
            for _ in range(t):
                Ab18(r, 0, 4, 8, 12); Ab18(r, 1, 5, 9, 13); Ab18(r, 2, 6, 10, 14); Ab18(r, 3, 7, 11, 15)
                Ab18(r, 0, 5, 10, 15); Ab18(r, 1, 6, 11, 12); Ab18(r, 2, 7, 12, 13); Ab18(r, 3, 4, 13, 14)
            for i in range(16): r[i] = (r[i] + e[i]) & 0xFFFFFFFF
            return r
        def rand_v():
            nonlocal l_St; rb = [4294967296, 4294965248, 53, 0, 2, 11, 8, 7]; e = Ab33(l_kt, rb[6]); t = e[l_St]; r = (rb[1] & e[l_St + rb[6]]) >> rb[5]
            if rb[7] == l_St: l_kt[12] = (l_kt[12] + 1) & 0xFFFFFFFF; l_St = rb[3]
            else: l_St += 1
            return (t + rb[0] * r) / math.pow(rb[4], rb[2])
        for i in range(12):
            num = int((2**32) * rand_v()) & 0xFFFFFFFF
            key.append(num); rounds = ((num & 15) + rounds) & 15
            keyStringArr.extend([(num >> (8*j)) & 255 for j in range(4)])
        rounds += 5
        r_list = [ord(c) for c in s_arr]
        e_con = [1196819126, 185100057, 3863347763, 3732962506] + key
        n, o, i, u = len(r_list) // 4, len(r_list) % 4, (len(r_list) + 3) // 4, [0] * ((len(r_list) + 3) // 4)
        for a in range(n): u[a] = r_list[4*a] | (r_list[4*a+1] << 8) | (r_list[4*a+2] << 16) | (r_list[4*a+3] << 24)
        if o > 0:
            u[n] = 0
            for c in range(o): u[n] |= r_list[4*n+c] << (8*c)
        state, o_idx = list(e_con), 0
        while o_idx + 16 <= len(u):
            i_arr = Ab33(state, rounds)
            state[12] = (state[12] + 1) & 0xFFFFFFFF
            for a_idx in range(16): u[o_idx + a_idx] ^= i_arr[a_idx]
            o_idx += 16
        if len(u) - o_idx > 0:
            s_arr_res = Ab33(state, rounds)
            for c in range(len(u) - o_idx): u[o_idx + c] ^= s_arr_res[c]
        for a in range(n):
            for j in range(4): r_list[4*a+j] = (u[a] >> (8*j)) & 255
        if o > 0:
            for d in range(o): r_list[4*n+d] = (u[n] >> (8*d)) & 255
        x = "".join(chr(c) for c in r_list)
        someVal = (sum(keyStringArr) + sum(ord(c) for c in x)) % (len(x) + 1)
        f_str = someRandomChar + x[:someVal] + "".join(chr(c) for c in keyStringArr) + x[someVal:]
        charSet = "u09tbS3UvgDEe6r-ZVMXzLpsAohTn7mdINQlW412GqBjfYiyk8JORCF5/xKHwacP="
        res = ""
        for i in range(3, len(f_str) + 1, 3):
            v = (ord(f_str[i-3]) << 16) | (ord(f_str[i-2]) << 8) | ord(f_str[i-1])
            res += charSet[(v & 16515072) >> 18] + charSet[(v & 258048) >> 12] + charSet[(v & 4032) >> 6] + charSet[v & 63]
        return res
