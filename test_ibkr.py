import asyncio
from ib_insync import IB

async def main():
    ib = IB()
    await ib.connectAsync("127.0.0.1", 7496, clientId=99, timeout=10)
    print("Connected:", ib.isConnected())
    ib.disconnect()

asyncio.run(main())