import asyncio
import aiohttp
import time

# You would dynamically fetch this from the MLB schedule endpoint: 
# https://statsapi.mlb.com/api/v1/schedule?sportId=1
GAME_PK = 824665 # Placeholder ID for a specific game

async def stream_live_game():
    url = f"https://statsapi.mlb.com/api/v1.1/game/{GAME_PK}/feed/live"
    last_event_time = None
    
    print("=" * 60)
    print(f" 🚀 Connecting to MLB Stats API | Game ID: {GAME_PK}")
    print("=" * 60)

    # ClientSession maintains a persistent connection pool, drastically reducing ping latency
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                loop_start = time.time()
                
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Dig into the JSON structure to find the current play
                        current_play = data.get("liveData", {}).get("plays", {}).get("currentPlay", {})
                        
                        # We use the end time of the play as our unique identifier
                        play_time = current_play.get("about", {}).get("endTime")
                        
                        if play_time != last_event_time and current_play:
                            last_event_time = play_time
                            
                            description = current_play.get("result", {}).get("description", "Event processing...")
                            pitch_speed = current_play.get("playEvents", [-1])[-1].get("pitchData", {}).get("startSpeed", "N/A")
                            
                            ping = time.time() - loop_start
                            print(f"⚡ [{ping:.3f}s ping] {description} | Pitch Speed: {pitch_speed} mph")
                            
                # Aggressive polling buffer: 0.5 seconds prevents your IP from getting rate-limited/banned
                await asyncio.sleep(0.5)

            except Exception as e:
                print(f" ❌ Connection hiccup: {e}. Retrying...")
                await asyncio.sleep(2)

if __name__ == "__main__":
    try:
        asyncio.run(stream_live_game())
    except KeyboardInterrupt:
        print("\n🔴 Stream closed manually.")