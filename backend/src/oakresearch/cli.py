from __future__ import annotations

import argparse
import asyncio
import json
import sys

from .db import create_pool, initialize_database, get_bootstrap_state


async def run_migrate() -> int:
    pool = await create_pool()
    try:
        async with pool.acquire() as conn:
            state = await initialize_database(conn)
        print(json.dumps(state, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        await pool.close()


async def run_status() -> int:
    pool = await create_pool()
    try:
        async with pool.acquire() as conn:
            state = await get_bootstrap_state(conn)
        print(json.dumps(state, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="oakresearch")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("migrate", help="Apply migrations and bootstrap default state")
    subcommands.add_parser("status", help="Print bootstrap state")

    args = parser.parse_args()

    if args.command == "migrate":
        raise SystemExit(asyncio.run(run_migrate()))
    if args.command == "status":
        raise SystemExit(asyncio.run(run_status()))

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
