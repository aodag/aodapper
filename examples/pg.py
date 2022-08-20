import asyncio
import asyncpg


async def run():
    conn = await asyncpg.connect(
        user="user",
        password="password",
        database="testdb",
        host="127.0.0.1",
    )
    values = await conn.fetch("SELECT CURRENT_TIMESTAMP")
    print(values)
    await conn.close()


asyncio.run(run())
