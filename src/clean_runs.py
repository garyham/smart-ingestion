"""Delete all flow runs from the Prefect server via the API.

Deletes are attempted in repeated passes rather than a single batch: Prefect
sometimes fails to delete a flow run that still has other rows referencing it
(e.g. a parent flow run of an ingest subflow that hasn't been removed yet - see
https://github.com/PrefectHQ/prefect/issues/16102), so each pass clears
whatever it can and retries the rest until nothing more can be removed.
"""

import asyncio
import sys

import httpx
from prefect.client.orchestration import PrefectClient
from prefect.settings import get_current_settings

_DEFAULT_API_URL = "http://127.0.0.1:4200/api"


async def _clear_flow_runs() -> None:
    api_url = get_current_settings().api.url or _DEFAULT_API_URL

    async with PrefectClient(api_url) as client:
        try:
            flow_runs = await client.read_flow_runs()
        except httpx.TransportError:
            print(
                f"Warning: could not reach the Prefect server at {api_url}. "
                "Is it running? (./ingest server)",
                file=sys.stderr,
            )
            return
        remaining = {run.id for run in flow_runs}
        total = len(remaining)
        if not total:
            print("No flow runs to delete.")
            return

        print(f"Deleting {total} flow run(s)...")
        pass_num = 0
        while remaining:
            pass_num += 1
            failed = set()
            for run_id in remaining:
                try:
                    await client.delete_flow_run(run_id)
                except Exception as exc:
                    failed.add(run_id)
            print(f"  pass {pass_num}: deleted {len(remaining) - len(failed)}, {len(failed)} remaining")

            if failed == remaining:
                print(f"Stopped: {len(failed)} flow run(s) could not be deleted:")
                for run_id in failed:
                    print(f"  {run_id}")
                break
            remaining = failed

        print(f"Deleted {total - len(remaining)}/{total} flow run(s).")


def main() -> None:
    asyncio.run(_clear_flow_runs())


if __name__ == "__main__":
    main()
