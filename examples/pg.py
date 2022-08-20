import asyncio
import dataclasses
import uuid
import asyncpg


@dataclasses.dataclass
class Dog:
    age: int
    id: uuid.UUID
    name: str
    weight: int


async def run():
    conn = await asyncpg.connect(
        user="user",
        password="password",
        database="testdb",
        host="127.0.0.1",
    )
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS dogs (id uuid PRIMARY KEY, age int, name text, weight int )"
    )
    dog = Dog(age=10, id=uuid.uuid4(), name="aodog", weight=5)
    await conn.execute(
        "INSERT INTO dogs (id, age, name, weight) VALUES ($1, $2, $3, $4)",
        dog.id,
        dog.age,
        dog.name,
        dog.weight,
    )
    values = await conn.fetch("SELECT * FROM dogs")
    for r in values:
        d = Dog(**dict(r.items()))
        print(d)
    await conn.execute(
        "DELETE FROM dogs WHERE id = $1",
        dog.id,
    )
    await conn.close()


asyncio.run(run())
