import asyncio
import websockets

async def send_and_receive(command):
    async with websockets.connect('ws://localhost:8765') as websocket:
        await websocket.send(command)
        message = await websocket.recv()
        print(f"Received message: {message}")
        return message
