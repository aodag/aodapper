import asyncio
import dataclasses
import typing
import uuid
import asyncpg


@dataclasses.dataclass
class Dog:
    age: int
    id: uuid.UUID
    name: str
    weight: int


T = typing.TypeVar("T")


async def query(
    conn: asyncpg.Connection,
    data_type: typing.Type[T],
    sql: str,
    params: typing.Sequence[typing.Any] = (),
) -> typing.AsyncGenerator[T, None]:
    values = await conn.fetch(sql, *params)
    for r in values:
        yield data_type(**dict(r.items()))


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
    async for d in query(conn, Dog, "SELECT * FROM dogs"):
        print(d)
    await conn.execute(
        "DELETE FROM dogs WHERE id = $1",
        dog.id,
    )
    await conn.close()


asyncio.run(run())
