import asyncio
import websockets
import urllib.parse
import re

async def zp_stoken(websocket, path):
    try:
        # 迭代接收来自客户端的消息
        async for message in websocket:
            response = f"Received message: {message}"
            print(response)
            # 在这里处理接收到的消息
            if(len(message)>60 and re.match('[a-zA-Z0-9]+',message)):
                # 将消息写入文件
                __zp_stoken__ = urllib.parse.quote(message)
                with open("./zp_stoken.txt", "w") as f:
                    f.write(__zp_stoken__)
                # 发送回复消息
                response = __zp_stoken__
            # 如果接收到的消息是 'refresh'，则回复同样的消息
            if(message == 'refresh'):
                response = message
            # 发送回复消息到客户端
            await websocket.send(response)
            print(f"Sent message: {response}")
    except websockets.exceptions.ConnectionClosedError as e:
        print(f"Connection closed with error: {e}")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        await websocket.close()

async def start_server():
    # 启动 WebSocket 服务器
    async with websockets.serve(zp_stoken, "localhost", 8765):
        # 保持服务器运行
        await asyncio.Future()

# 如果作为脚本运行，启动 WebSocket 服务器并开始事件循环
if __name__ == '__main__':
    asyncio.run(start_server())
