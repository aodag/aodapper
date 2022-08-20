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
    prefix: str,
    sql: str,
    params: typing.Sequence[typing.Any] = (),
) -> typing.AsyncGenerator[T, None]:
    p = prefix + "_"
    values = await conn.fetch(sql, *params)
    for r in values:
        yield data_type(
            **dict((k[len(p) :], v) for k, v in r.items() if k.startswith(p))
        )


def columns(
    data_type: typing.Type,
    prefix: typing.Optional[str] = None,
    excludes: typing.Sequence[str] = [],
) -> str:
    return ", ".join(
        [
            (f"{prefix}.{f.name} AS {prefix}_{f.name}" if prefix else f.name)
            for f in dataclasses.fields(data_type)
            if f.name not in excludes
        ]
    )


async def run() -> None:
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
    async for d in query(
        conn,
        Dog,
        "d1",
        f"SELECT {columns(Dog, prefix='d1')}, {columns(Dog, prefix='d2')} FROM dogs AS d1 CROSS JOIN dogs AS d2",
    ):
        print(d)
    await conn.execute(
        "DELETE FROM dogs WHERE id = $1",
        dog.id,
    )
    await conn.execute("DELETE FROM dogs")
    await conn.close()


asyncio.run(run())
