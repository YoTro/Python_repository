import websocket
import struct

def on_message(ws, message):
    print("++Rcv raw: {}".format(message))

    # 解析WebSocket数据帧
    opcode_and_flags, payload_len = struct.unpack("!BB", message[:2])
    opcode = opcode_and_flags & 0x0F
    is_fin = opcode_and_flags & 0x80
    is_masked = payload_len & 0x80

    # 解析payload长度
    payload_len &= 0x7F
    if payload_len == 126:
        payload_len, = struct.unpack("!H", message[2:4])
        offset = 4
    elif payload_len == 127:
        payload_len, = struct.unpack("!Q", message[2:10])
        offset = 10
    else:
        offset = 2

    # 解析payload数据
    if is_masked:
        masking_key = message[offset:offset+4]
        offset += 4
        masked_payload = message[offset:offset+payload_len]
        unmasked_payload = bytearray(payload_len)
        for i in range(payload_len):
            unmasked_payload[i] = masked_payload[i] ^ masking_key[i % 4]
        payload = unmasked_payload
    else:
        payload = message[offset:offset+payload_len]

    # 打印解析结果
    print("++Rcv decoded: fin={} opcode={} data={}".format(is_fin, opcode, payload))

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("WebSocket closed")

def on_open(ws):
    ws.send("Hello, Server!")

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://toryunbot.com/websocket",
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
