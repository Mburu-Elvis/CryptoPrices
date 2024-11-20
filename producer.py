from confluent_kafka import Producer
import socket
import asyncio
import json
import aiohttp
from aiohttp import ClientSession
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

coins = ['BTC-USD', 'ETH-USD', 'USDT-USD', 'SOL-USD', 'BNB-USD']

conf = {
    'bootstrap.servers': os.environ.get('BOOTSTRAP_SERVERS'),
    'client.id': socket.gethostname(),
}

topic = os.environ.get('TOPIC')

producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced to %s partition: [%s] at offset: %s" % (msg.topic(), msg.partition(), msg.offset()))

async def fetch_price(url: str, session: ClientSession, **kwargs) -> dict:
    price_time = datetime.now()
    async with session.get(url=url, **kwargs) as resp:
        resp.raise_for_status()
        price_obj = await resp.json()
        price_obj['data']['price_time'] = str(price_time)
        return price_obj['data']

async def crypto_prices(coins: list) -> None:
    async with ClientSession() as session:
        tasks = [fetch_price(url=f"https://api.coinbase.com/v2/prices/{coin}/spot", session=session) for coin in coins]
        prices = await asyncio.gather(*tasks)
        return prices

async def produce(data):
    loop = asyncio.get_event_loop()
    
    def async_ack_callback(err, msg):
        # Use asyncio's event loop to execute the callback asynchronously
        asyncio.ensure_future(async_ack_callback_async(err, msg))

    # The actual async function to run the callback
    async def async_ack_callback_async(err, msg):
        await loop.run_in_executor(None, acked, err, msg)
    
    while True:
        for crypto in data:
            json_dump = json.dumps(crypto)
            producer.produce(topic, key=crypto['base'], value=json_dump.encode('utf-8'), callback=async_ack_callback)
        producer.flush()
        await asyncio.sleep(5)

async def main():
    prices = await crypto_prices(coins)
    await produce(prices)

if __name__ == '__main__':
    import time 
    start = time.perf_counter()

    print("start: {}".format(start))
    asyncio.run(main())
    end = time.perf_counter() - start
    print(f"took {end:0.2f}")