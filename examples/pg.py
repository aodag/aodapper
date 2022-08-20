import asyncio
import dataclasses
import typing
import uuid
import asyncpg


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
    rel_prefixes = []
    for rel in dataclasses.fields(data_type):
        if not dataclasses.is_dataclass(rel.type):
            continue
        rel_prefixes.append((rel.type.__name__.lower(), rel.type))

    for r in values:
        attrs = dict((k[len(p) :], v) for k, v in r.items() if k.startswith(p))
        for rel_prefix, rel_type in rel_prefixes:
            pp = rel_prefix + "_"
            rel = rel_type(
                **dict((k[len(pp) :], v) for k, v in r.items() if k.startswith(pp))
            )
            attrs[rel_prefix] = rel
        yield data_type(**attrs)


def columns(
    data_type: typing.Type,
    prefix: typing.Optional[str] = None,
    excludes: typing.Sequence[str] = [],
) -> str:
    cols = [
        (f"{prefix}.{f.name} AS {prefix}_{f.name}" if prefix else f.name)
        for f in dataclasses.fields(data_type)
        if f.name not in excludes and not dataclasses.is_dataclass(f.type)
    ]
    for rel in dataclasses.fields(data_type):
        if rel.name in excludes or not dataclasses.is_dataclass(rel.type):
            continue
        cols += [
            f"{rel.type.__name__.lower()}.{f.name} AS {rel.type.__name__.lower()}_{f.name}"
            for f in dataclasses.fields(rel.type)
        ]

    return ", ".join(cols)


@dataclasses.dataclass
class Department:
    id: str
    name: str


@dataclasses.dataclass
class Job:
    id: str
    name: str


@dataclasses.dataclass
class Employee:
    id: uuid.UUID
    first_name: str
    last_name: str
    job: Job
    department: Department


async def example() -> None:
    conn = await asyncpg.connect(
        user="user",
        password="password",
        database="testdb",
        host="127.0.0.1",
    )
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS departments (id text PRIMARY KEY, name text)"
    )
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS jobs (id text PRIMARY KEY, name text)"
    )
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS employee "
        "(id uuid PRIMARY KEY, first_name text, last_name text, job_id text REFERENCES jobs(id), dept_id text REFERENCES departments(id))"
    )
    await conn.execute("DELETE FROM employee")
    await conn.execute("DELETE FROM jobs")
    await conn.execute("DELETE FROM departments")
    await conn.execute("INSERT INTO jobs (id, name) VALUES ('programmer', 'プログラマー')")
    await conn.execute("INSERT INTO departments (id, name) VALUES ('dev1', '開発第一')")
    employee_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO employee "
        "(id, first_name, last_name, job_id, dept_id)"
        "VALUES ($1, $2, $3, $4, $5)",
        employee_id,
        "atsushi",
        "odagiri",
        "programmer",
        "dev1",
    )
    async for e in query(
        conn,
        Employee,
        "e",
        f"SELECT {columns(Employee, prefix='e')} "
        "FROM employee AS e  "
        "JOIN jobs AS job ON job.id = e.job_id "
        "JOIN departments AS department ON department.id = e.dept_id "
        " WHERE e.id = $1",
        (employee_id,),
    ):
        print(e)
    await conn.close()


asyncio.run(example())
