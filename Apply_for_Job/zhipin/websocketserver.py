import asyncio
import websockets
import urllib.parse
async def hello(websocket, path):
    name = await websocket.recv()
    if name == 'refresh':
        await websocket.send('refresh')
        print(f"Received {name}")
    else:
        zp_stoken = urllib.parse.quote(name)
        with open("./zp_stoken.txt", "w") as f:
            f.write(zp_stoken)
        print(f"Received {zp_stoken}")

async def start_server():
    async with websockets.serve(hello, "localhost", 8765):
        await asyncio.Future()  # keep server running

# 启动 WebSocket 服务器并开始事件循环
if __name__ == '__main__':
    asyncio.run(start_server())
