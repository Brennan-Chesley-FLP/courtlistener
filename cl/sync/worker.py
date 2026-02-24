"""In-process Prefect worker for running sync flows inside a Django process.

Simplified version of en_banc's InProcessWorker — no Litestream, just
load the flow from the deployment entrypoint and run it in the current
event loop.  Django ORM is available because the worker runs inside
``manage.py sync_worker``.

Usage::

    prefect work-pool create sync-pool --type in-process
    python manage.py sync_worker
"""

from __future__ import annotations

import logging
from typing import Optional

import anyio
import anyio.abc
from prefect.client.schemas.objects import FlowRun
from prefect.exceptions import Abort, Pause
from prefect.flow_engine import run_flow, run_flow_async
from prefect.flows import load_flow_from_flow_run
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseWorker,
    BaseWorkerResult,
)

logger = logging.getLogger(__name__)


class SyncWorkerResult(BaseWorkerResult):
    """Result of an in-process sync flow run."""


class SyncWorker(BaseWorker):
    """Execute sync flows in a worker thread with Django ORM access.

    Runs inside a Django management command so the flow function has
    full access to the Django ORM and the warehouse database connection.

    Sync flows are dispatched to a worker thread via anyio so that
    Django's synchronous ORM calls don't conflict with the async
    event loop that Prefect's worker infrastructure requires.
    """

    type = "in-process"
    job_configuration = BaseJobConfiguration

    _description = "Execute sync flows in the current async event loop."
    _display_name = "Sync In-Process"

    async def run(
        self,
        flow_run: FlowRun,
        configuration: BaseJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> SyncWorkerResult:
        identifier = str(flow_run.id)

        if task_status is not None:
            task_status.started(identifier)

        try:
            flow = await load_flow_from_flow_run(
                flow_run, ignore_storage=True
            )
        except Exception:
            logger.exception(
                "Failed to load flow for run %s", flow_run.id
            )
            return SyncWorkerResult(
                status_code=1, identifier=identifier
            )

        try:
            if flow.isasync:
                state = await run_flow_async(
                    flow,
                    flow_run=flow_run,
                    parameters=flow_run.parameters,
                    return_type="state",
                )
            else:
                # Run sync flows in a worker thread so Django's
                # synchronous ORM calls work within the async event loop.
                state = await anyio.to_thread.run_sync(
                    lambda: run_flow(
                        flow,
                        flow_run=flow_run,
                        parameters=flow_run.parameters,
                        return_type="state",
                    )
                )
        except (Abort, Pause):
            raise
        except Exception:
            logger.exception(
                "Unexpected engine error in flow run %s", flow_run.id
            )
            return SyncWorkerResult(
                status_code=1, identifier=identifier
            )

        status_code = 0 if state.is_completed() else 1
        return SyncWorkerResult(
            status_code=status_code, identifier=identifier
        )
