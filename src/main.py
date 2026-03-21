from __future__ import annotations

import asyncio

from src.app import run_application


def main() -> None:
    asyncio.run(run_application())


if __name__ == "__main__":
    main()
