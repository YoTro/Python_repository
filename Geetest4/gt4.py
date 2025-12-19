import json
import math
import hashlib
import binascii
import uuid
import re
import os
import time
import random
from Crypto.PublicKey import RSA
from Crypto.Cipher import AES
from Crypto.Cipher import PKCS1_v1_5
from Crypto.Util.Padding import pad
import requests
from urllib.parse import parse_qs
import tkinter as tk
from PIL import Image, ImageTk
from io import BytesIO
from urllib.parse import urlencode

class GeetestV4:
    """极验V4验证码处理类"""
    
    # ================== 常量配置 ==================
    RSA_PUBLIC_KEY = {
        "n": "00C1E3934D1614465B33053E7F48EE4EC87B14B95EF88947713D25EECBFF7E74C7977D02DC1D9451F79DD5D1C10C29ACB6A9B4D6FB7D0A0279B6719E1772565F09AF627715919221AEF91899CAE08C0D686D748B20A3603BE2318CA6BC2B59706592A9219D0BF05C9F65023A21D2330807252AE0066D59CEEFA5F2748EA80BAB81",
        "e": "10001"
    }
    
    DEFAULT_CONFIG = {
        "pt": 1,
        "payload_protocol": 1,
        "ep": "123",
        "biht": "1426265548",
        "gee_guard": {
            "roe": {
                "aup": "3", "sep": "3", "egp": "3", "auh": "3",
                "rew": "3", "snh": "3", "res": "3", "cdc": "3"
            }
        },
        "LldF": "7rCZ",
        "em": {
            "ph": 0, "cp": 0, "ek": "11", "wd": 1,
            "nt": 0, "si": 0, "sc": 0
        },
        "lang": "zh",
        "geetest": "captcha"
    }
    
    def __init__(self, captcha_id, session=None, cookies=None, headers=None):
        """
        初始化极验V4实例
        
        Args:
            captcha_id: 验证码ID
            session: requests.Session对象，可选
            cookies: 请求cookies，可选
            headers: 请求头，可选
        """
        self.captcha_id = captcha_id
        self.challenge = str(uuid.uuid4())
        self.session = session or requests.Session()
        # 设置默认cookie
        if not cookies:
            cookies = {
                'captcha_v4_user': str(uuid.uuid4()).replace('-', '')
            }
        self.cookies = cookies
        self.headers = self._get_default_headers()
        if headers:
            self.headers.update(headers)
        self.config = self.DEFAULT_CONFIG.copy()
        self.config["captcha_id"] = captcha_id
        self.symmetric_key = self.generate_random_hex16()
        # 更新session的cookies
        if cookies:
            self.session.cookies.update(cookies)
    
    def _get_default_headers(self):
        """获取默认请求头"""
        return {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "referer": "https://gt4.geetest.com/",
            "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "script",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        }
    
    def _parse_callback_response(self, response_text):
        """
        解析回调函数格式的响应
        
        Args:
            response_text: 响应文本，格式如：geetest_xxx({...})
            
        Returns:
            解析后的字典数据
        """
        # 提取JSON部分
        match = re.search(r'\((.*)\)$', response_text)
        if match:
            json_str = match.group(1)
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                # 尝试修复可能的JSON格式问题
                json_str = re.sub(r',\s*}', '}', json_str)
                json_str = re.sub(r',\s*]', ']', json_str)
                return json.loads(json_str)
        else:
            # 如果没有回调函数包裹，直接解析JSON
            return json.loads(response_text)
    
    def load(self, callback=None, risk_type="word", parse_response=True):
        """
        加载验证码
        
        Args:
            callback: 回调函数名
            risk_type: 风险类型
            parse_response: 是否解析响应内容
            
        Returns:
            如果parse_response=True: 返回解析后的字典
            如果parse_response=False: 返回requests.Response对象
        """
        if callback is None:
            callback = f"geetest_{int(time.time() * 1000)}"
            
        url = "https://gcaptcha4.geetest.com/load"
        
        params = {
            "callback": callback,
            "captcha_id": self.captcha_id,
            "challenge": self.challenge,
            "client_type": "web",
            "risk_type": risk_type,
            "lang": "zh-cn",
            "pt": "1"
        }

        response = self.session.get(
            url, 
            params=params, 
            headers=self.headers
        )
        print(f"Load URL: {response.url}")
        print(f"状态码: {response.status_code}")
        
        if parse_response:
            return self._parse_callback_response(response.text)
        else:
            return response
    
    def verify(self, w, load_data, callback=None):
        """
        提交验证
        
        Args:
            w: 加密后的w参数
            callback: 回调函数名
            
        Returns:
            验证结果
        """
        if callback is None:
        # 生成 13 位毫秒级时间戳
            callback = f"geetest_{int(time.time() * 1000)}"
            
        url = "https://gcaptcha4.geetest.com/verify"
        data = load_data.get("data", {})
        params = {
            "callback": callback,
            "captcha_id": self.captcha_id,
            "client_type": "web",
            "lot_number": data.get("lot_number"),
            "risk_type": "word",
            "payload": data.get("payload"),
            "process_token": data.get("process_token"),
            "payload_protocol": "1",
            "pt": "1",
            "w": w

        }
        
        # 注意：requests.get 的 params 会被拼接到 URL 后, post会报错
        response = self.session.get(
            url,
            params=params,
            headers=self.headers,
        )
        print(response.url)
        print(f"验证状态码: {response.status_code}")
        print("验证响应:")        
        return self._parse_callback_response(response.text)
    
    def parse_load_response(self, response_data):
        """
        解析load响应，提取关键信息
        
        Args:
            response_data: load返回的字典数据
            
        Returns:
            提取的关键信息字典
        """
        if response_data.get("status") != "success":
            raise ValueError(f"加载失败: {response_data}")
        
        data = response_data.get("data", {})
        
        # 提取必要信息
        result = {
            "lot_number": data.get("lot_number"),
            "pow_detail": data.get("pow_detail", {}),
            "captcha_type": data.get("captcha_type"),
            "imgs": data.get("imgs"),
            "ques": data.get("ques", []),
            "payload": data.get("payload"),
            "process_token": data.get("process_token"),
            "payload_protocol": data.get("payload_protocol", 1),
            "static_path": data.get("static_path"),
            "gct_path": data.get("gct_path"),
            "custom_theme": data.get("custom_theme", {})
        }
        
        return result
    
    def extract_image_urls(self, response_data):
        """
        提取图片URL
        
        Args:
            response_data: load返回的字典数据
            
        Returns:
            图片URL列表
        """
        data = response_data.get("data", {})
        base_url = "https://static.geetest.com/"
        
        # 主图片
        main_img = f"{base_url}/{data.get('imgs', '')}" if data.get('imgs') else None
        
        # 问题图片
        ques_imgs = []
        for ques in data.get("ques", []):
            ques_imgs.append(f"{base_url}/{ques}")
        
        return {
            "main_img": main_img,
            "ques_imgs": ques_imgs
        }

    # ================== 工具方法 ==================
    @staticmethod
    def generate_random4_hex():
        """生成4位随机16进制数"""
        return hex(int((1 + random.random()) * 65536) & 0xFFFF)[2:].zfill(4)

    @staticmethod
    def generate_random_hex16():
        """生成16位随机16进制数"""
        return ''.join(GeetestV4.generate_random4_hex() for _ in range(4))

    @staticmethod
    def parse_string_to_word_array(s: str):
        """字符串转word数组（JS风格）"""
        words = []
        for i in range(0, len(s), 4):
            w = 0
            for j in range(4):
                if i + j < len(s):
                    w |= (ord(s[i + j]) & 0xff) << (24 - j * 8)
            words.append(w)
        return {"words": words, "sigBytes": len(s)}

    @staticmethod
    def array_to_hex(arr):
        """字节数组转16进制字符串"""
        return ''.join(f"{b:02x}" for b in arr)

    # ================== 哈希方法 ==================
    @staticmethod
    def sha256_hex(s):
        return hashlib.sha256(s.encode()).hexdigest()

    @staticmethod
    def sha1_hex(s):
        return hashlib.sha1(s.encode()).hexdigest()

    @staticmethod
    def md5_hex(s):
        return hashlib.md5(s.encode()).hexdigest()

    # ================== 动态字符串生成 ==================
    @staticmethod
    def generate_dynamic_strings(lot_number):
        """生成动态字符串（用于混淆）"""
        n = lot_number
        return {
            n[7:13]: {
                n[1:5] + n[24:28]: {
                    n[3:5] + n[16:18]: n[16:20]
                }
            }
        }

    # ================== POW 验证 ==================
    def generate_pow_msg(self, lot_number, pow_detail):
        """生成POW消息"""
        return (
            f"{pow_detail.get('version','1')}|"
            f"{pow_detail.get('bits',8)}|"
            f"{pow_detail.get('hashfunc','sha256')}|"
            f"{pow_detail['datetime']}|"
            f"{self.captcha_id}|"
            f"{lot_number}||"
            
        )

    def generate_pow_sign(self, pow_msg, bits):
        """
        针对极验V4优化的POW生成
        1. 生成符合格式的16位随机十六进制nonce
        2. 使用字节流操作，跳过hexdigest字符串转换
        """
        base_bytes = pow_msg.encode('utf-8')
        leading_bytes = bits // 8
        remaining_bits = bits % 8
        mask = (0xFF << (8 - remaining_bits)) & 0xFF
        
        # 预先准备随机种子（使用os.urandom保证随机性，之后内部递增提高速度）
        seed = int.from_bytes(os.urandom(8), 'big')
        
        i = 0
        while True:
            # 快速生成16位十六进制字符串的字节形式 (例如: "5e4c07231b2e660d")
            # 这种方式比 hex(seed + i) 快，因为它固定长度且不带 0x
            nonce_str = f"{(seed + i) & 0xFFFFFFFFFFFFFFFF:016x}"
            nonce_bytes = nonce_str.encode('utf-8')
            
            # 计算摘要 (digest() 返回 32 字节二进制)
            res_bytes = hashlib.sha256(base_bytes + nonce_bytes).digest()
            
            # 核心校验逻辑：字节比对比字符串快得多
            is_match = True
            # 1. 检查完整字节
            for j in range(leading_bytes):
                if res_bytes[j] != 0:
                    is_match = False
                    break
            
            if is_match:
                # 2. 检查剩余的比特位
                if remaining_bits == 0 or (res_bytes[leading_bytes] & mask) == 0:
                    return pow_msg + nonce_str, res_bytes.hex()
            
            i += 1
            if i > 1000000: # 定期重置种子防止循环过长
                seed = int.from_bytes(os.urandom(8), 'big')
                i = 0

        raise RuntimeError("POW生成失败")

    # ================== RSA 加密 ==================
    @staticmethod
    def rsa_encrypt_js_style(message: str) -> str:
        """RSA加密（模拟JS实现）"""
        n = int(GeetestV4.RSA_PUBLIC_KEY["n"][2:], 16)
        e = int(GeetestV4.RSA_PUBLIC_KEY["e"], 16)

        key = RSA.construct((n, e))
        message_bytes = message.encode('utf-8')
        cipher = PKCS1_v1_5.new(key)
        encrypted_bytes = cipher.encrypt(message_bytes)
        encrypted_hex = binascii.hexlify(encrypted_bytes).decode('utf-8')

        return encrypted_hex

    # ================== AES 加密 ==================
    @staticmethod
    def aes_128_cbc_encrypt(plaintext, key_wa, iv_wa):
        """AES-128-CBC加密"""
        key = b''.join(
            key_wa["words"][i].to_bytes(4, "big")
            for i in range(4)
        )
        iv = b''.join(
            iv_wa["words"][i].to_bytes(4, "big")
            for i in range(4)
        )

        cipher = AES.new(key, AES.MODE_CBC, iv)
        ct = cipher.encrypt(pad(plaintext.encode(), 16))
        return list(ct)

    # ================== 主要加密方法 ==================
    def generate_w(self, data):
        """
        生成最终的w参数
        
        Args:
            data: 需要加密的数据
            
        Returns:
            加密后的w参数
        """
        

        # RSA加密对称密钥
        rsa_part = self.rsa_encrypt_js_style(self.symmetric_key)

        # JSON序列化
        json_str = json.dumps(data, separators=(",", ":"))

        # AES加密数据
        key_wa = self.parse_string_to_word_array(self.symmetric_key)
        iv_wa = self.parse_string_to_word_array("0000000000000000")
        aes_bytes = self.aes_128_cbc_encrypt(json_str, key_wa, iv_wa)
        aes_hex = self.array_to_hex(aes_bytes)

        return aes_hex + rsa_part

    def generate_validate_data(self, load_response_data, passtime, userresponse, device_id=""):
        """
        从load响应生成验证数据
        
        Args:
            load_response_data: load返回的完整响应数据
            passtime: 通过时间
            userresponse: 用户响应坐标
            device_id: 设备ID
            
        Returns:
            包含w参数的字典
        """
        # 解析load响应
        parsed_data = self.parse_load_response(load_response_data)
        
        lot_number = parsed_data["lot_number"]
        pow_detail = parsed_data["pow_detail"]
        
        # 生成POW
        pow_msg, pow_sign = self.generate_pow_sign(
            self.generate_pow_msg(lot_number, pow_detail),
            pow_detail["bits"]
        )

        # 构造数据
        data = {
            "passtime": passtime,
            "userresponse": userresponse,
            "device_id": device_id,
            "lot_number": lot_number,
            "pow_msg": pow_msg,
            "pow_sign": pow_sign,
            "geetest": self.config["geetest"],
            "lang": self.config["lang"],
            "ep": self.config["ep"],
            "biht": self.config["biht"],
            "gee_guard": self.config["gee_guard"],
            "LldF": self.config["LldF"],
            **self.generate_dynamic_strings(lot_number),
            "em": self.config["em"]
        }
        print(data)
        # 生成w参数
        w = self.generate_w(data)
        
        return {
            "data": data,
            "w": w,
            "w_length": len(w),
            "lot_number": lot_number,
            "process_token": parsed_data["process_token"]
        }

class InteractiveClick:
    """Tkinter 交互类，用于获取用户点击坐标，并支持显示多张图片"""
    def __init__(self, main_image_url, ques_image_urls, session):
        self.root = tk.Tk()
        self.root.title("请点击图片中的指定位置 (极验V4)")
        self.coords = []
        self.session = session
        

        # 1. 下载并加载所有图片
        self.original_main_img = self._download_image(main_image_url)
        self.original_ques_imgs = [self._download_image(url) for url in ques_image_urls]
        self.start_time = time.time() # 记录开始时间
        # 2. 计算合并后图片的尺寸
        main_width, main_height = self.original_main_img.size
        
        # 问题图片总宽度，加上图片之间的间隔（假设每个间隔5像素）
        ques_total_width = sum(img.width for img in self.original_ques_imgs)
        ques_max_height = max((img.height for img in self.original_ques_imgs), default=0)
        
        # 计算合并后的总宽度和总高度
        # 如果有副图，则合并图的宽度取主图和副图总宽度的最大值
        combined_width = max(main_width, ques_total_width + (len(self.original_ques_imgs) - 1) * 5) if self.original_ques_imgs else main_width
        combined_height = main_height + (ques_max_height + 10 if self.original_ques_imgs else 0) # 10像素间隔

        # 3. 创建一张新的大图，并将所有图片粘贴上去
        combined_img = Image.new('RGB', (combined_width, combined_height), (255, 255, 255)) # 白色背景
        
        # 粘贴主图片
        combined_img.paste(self.original_main_img, (0, 0))
        
        # 粘贴问题图片
        current_x_offset = 0
        if self.original_ques_imgs:
            for q_img in self.original_ques_imgs:
                combined_img.paste(q_img, (current_x_offset, main_height + 10)) # 10像素作为主图和问题图的间隔
                current_x_offset += q_img.width + 5 # 5像素作为问题图片之间的间隔
        
        self.tk_img = ImageTk.PhotoImage(combined_img)
        self.img_width = combined_img.width  # 更新为合并后图片的宽度
        self.img_height = combined_img.height # 更新为合并后图片的高度

        # UI 布局
        self.canvas = tk.Canvas(self.root, width=self.img_width, height=self.img_height)
        self.canvas.pack()
        self.canvas.create_image(0, 0, anchor=tk.NW, image=self.tk_img)
        
        # 绑定点击事件
        self.canvas.bind("<Button-1>", self.on_click)
        
        # 说明标签
        self.label = tk.Label(self.root, text="点击完成后，关闭窗口即可提交。")
        self.label.pack()

    def _download_image(self, url):
        """下载图片并将透明部分转为白色"""
        try:
            resp = self.session.get(url)
            img = Image.open(BytesIO(resp.content))
            
            # 如果是 RGBA (带透明度) 或 P 模式
            if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
                # 创建白色背景图层
                background = Image.new('RGB', img.size, (255, 255, 255))
                # 将原图粘贴上去，使用自身的 Alpha 通道作为 mask
                background.paste(img, (0, 0), img.convert('RGBA'))
                return background
            
            return img.convert('RGB')
        except Exception as e:
            print(f"图片处理失败: {e}")
            return Image.new('RGB', (50, 50), (200, 200, 200))

    def on_click(self, event):
        # 严格遵循 JS 逻辑：
        container_width = 300
        container_height = 200 # 如果是点选文字，通常是200
        # 1. 计算相对百分比 (0-100 之间)
        relative_x = (event.x / container_width) * 100
        relative_y = (event.y / container_height) * 100
        
        # 2. 模拟 JS 的 Math.round(100 * i)
        # 极验 V4 最终 userresponse 里的值通常是整数
        final_x = round(relative_x * 100)
        final_y = round(relative_y * 100)
        
        self.coords.append([final_x, final_y])
        
        # 可视化反馈
        self.canvas.create_oval(event.x-2, event.y-2, event.x+2, event.y+2, fill="red")
        print(f"原始点击: ({event.x}, {event.y}) -> 转换后坐标: [{final_x}, {final_y}]")

    def get_result(self):
        self.root.mainloop()
        self.end_time = time.time() # 记录结束时间
        passtime = round((self.end_time - self.start_time) * 1000) # 计算毫秒
        return self.coords, passtime

# ================== 使用示例 ==================
if __name__ == "__main__":  
    print("\n=== 实际使用流程 ===")
    print("1. 初始化GeetestV4实例")
    captcha_id = "54088bb07d2df3c46b79f80300b0abbe"
    geetest = GeetestV4(captcha_id)
    print("2. 调用load()方法获取验证码数据")
    parsed_response=geetest.load()
    print("解析后的响应数据:")
    print(json.dumps(parsed_response, indent=2, ensure_ascii=False))
    # 提取图片URL
    image_urls = geetest.extract_image_urls(parsed_response)
    print("cookies:", geetest.session.cookies.get_dict())

    main_img = image_urls['main_img']
    ques_imgs = image_urls['ques_imgs']
    print("3. 显示验证码给用户，获取用户交互数据")
    # 5. 生成验证数据（模拟用户交互后）
    if main_img:
            
            # 启动交互窗口，传入所有图片URL
            interactor = InteractiveClick(main_img, ques_imgs, geetest.session)
            user_coords, passtime = interactor.get_result() # 这会阻塞直到窗口关闭
            if not user_coords:
                print("未获取到坐标，程序退出")
            else:
                print(f"4. 提交坐标: {user_coords}, 耗时: {passtime} ms")
                
                # 6. 生成验证数据
                result = geetest.generate_validate_data(
                    load_response_data=parsed_response,
                    passtime=passtime, # 使用实际计算的耗时
                    userresponse= user_coords, 
                    device_id=""
                )
                print("cookies:", geetest.session.cookies.get_dict())

                print(result['w'])
                # 7. 提交验证
                print("5. 调用verify()提交验证...")
                verify_result = geetest.verify(result['w'], parsed_response)
                print(f"最终结果: {verify_result}")
    else:
        print("未能获取到主图片，请检查 captcha_id 或网络")