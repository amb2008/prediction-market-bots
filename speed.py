import asyncio 
import websockets
import time
async def continuous_latency(uri, iterations=10):
    async with websockets.connect(uri) as websocket:
        latencies = []
        for _ in range(iterations):
            start = time.time()
            await websocket.send("ping")
            await websocket.recv()
            end = time.time()
            latency = (end - start) * 1000
            latencies.append(latency)
            await asyncio.sleep(0.1)  # Measure every second
            
        avg_latency = sum(latencies) / len(latencies)
        print(f"Average latency over {iterations} messages: {avg_latency:.2f} ms")

asyncio.run(continuous_latency("wss://echo.websocket.org"))