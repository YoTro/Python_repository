# Geetest V4 文字点选验证码逆向分析文档

> 对 Geetest V4 点选验证码的**完整协议流程、关键参数、加密与校验逻辑**进行逆向分析整理。

---

## 1. 总体流程概览

Geetest V4 在 Web 端的完整校验流程如下：

```
load → 展示验证码 → 用户交互 → POW → 构造业务数据
   → AES-128-CBC 加密 → RSA 加密对称密钥 → 拼接 w → verify
```

其中 **w 参数**是最终校验的核心。

---

## 2. load 接口分析

### 2.1 请求信息

- URL
```
https://gcaptcha4.geetest.com/load
```

- 关键参数

| 参数 | 说明 |
|----|----|
| captcha_id | 验证码 ID（业务方固定） |
| challenge | UUID4，每次刷新不同 |
| risk_type | 通常为 word |
| client_type | web |
| callback | JSONP 回调 |

---

### 2.2 load 响应关键字段

```json
{
  "lot_number": "...",
  "pow_detail": { ... },
  "imgs": "...",
  "ques": [ ... ],
  "payload": "...",
  "process_token": "..."
}
```

| 字段 | 作用 |
|----|----|
| lot_number | 本次验证唯一标识（强绑定） |
| pow_detail | POW 难度与规则 |
| imgs / ques | 验证码图片 |
| payload | 后续 verify 必须携带 |
| process_token | 会话绑定 token |

---

## 3. 用户交互数据

### 3.1 坐标计算方式

前端并非直接上传像素坐标，而是：

```
relative = click / container_size * 100
userresponse = round(relative * 100)
```

最终坐标为 **整数百分比 ×100**。

### 3.2 passtime

- 单位：毫秒
- 计算方式：
```
窗口打开 → 窗口关闭的真实时间差
```

该字段属于 **弱校验但必须合理**。

---

## 4. POW（工作量证明）机制

### 4.1 POW 消息结构

```
version|bits|hashfunc|datetime|captcha_id|lot_number||nonce
```

其中 nonce 需要满足 hash 约束。

---

### 4.2 POW 校验规则

- 使用 hashfunc（通常为 sha256）
- 对 `pow_msg + nonce` 计算 digest
- 要求：
  - digest 前 `bits` 位为 0

> V4 使用 **indexOf 判断**。

---

## 5. w 参数结构（核心）

### 5.1 整体结构

```
w = AES(ciphertext hex) + RSA(symmetric_key hex)
```

---

### 5.2 AES 加密部分

- 算法：AES-128-CBC
- Key：16 位随机 hex
- IV："0000000000000000"
- 明文：JSON 压缩格式

```json
{
  "passtime": 1234,
  "userresponse": [[x,y]],
  "lot_number": "...",
  "pow_msg": "...",
  "pow_sign": "...",
  "ep": "...",
  "gee_guard": { ... },
  "em": { ... }
}
```

---

### 5.3 RSA 加密部分

- 公钥固定（前端写死）
- 填充：PKCS1 v1.5
- 明文：AES 对称密钥
- 输出：hex 字符串

---

## 6. verify 接口分析

### 6.1 请求信息

- URL
```
https://gcaptcha4.geetest.com/verify
```

- 关键参数

| 参数 | 是否必须 |
|----|----|
| captcha_id | 是 |
| lot_number | 是 |
| payload | 是 |
| process_token | 是 |
| w | 是 |
| callback | 是 |

---

### 6.2 校验重点

1. w 解密正确性
2. POW 是否有效
3. lot_number / payload / token 是否一致
4. passtime 与行为合理性

---

## 7. 强 / 弱校验字段总结

### 强校验（必须真实）
- lot_number
- payload
- process_token
- pow_msg / pow_sign
- w 加密结构

### 弱校验（可控但需合理）
- passtime
- userresponse
- em / gee_guard

---

## 8. 结论

- Geetest V4 本质是：
  - **POW + 行为数据 + 对称加密 + 非对称密钥保护**
- 真实安全性依赖于：
  - 前端完整性
  - 行为模型
  - 设备与 IP 风险

本代码已完整复现 **协议层验证逻辑**，可用于：
- 验证失败分析
- 安全研究
- 行为风控研究

---

（完）

