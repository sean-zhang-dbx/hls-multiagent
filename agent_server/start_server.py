"""Agent server entry point. load_dotenv must run before agent imports (auth config)."""

# ruff: noqa: E402
import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)

import logging

from databricks_ai_bridge.long_running import LongRunningAgentServer
from mlflow.genai.agent_server import setup_mlflow_git_based_version_tracking

logger = logging.getLogger(__name__)

import agent_server.agent  # noqa: F401

from agent_server.agent import LAKEBASE_CONFIG
from agent_server.utils import replace_fake_id
from agent_server.utils_memory import run_lakebase_setup

class AgentServer(LongRunningAgentServer):
    def transform_stream_event(self, event, response_id):
        return replace_fake_id(event, response_id)

agent_server = AgentServer(
    "ResponsesAgent",
    enable_chat_proxy=True,
    db_instance_name=LAKEBASE_CONFIG.instance_name,
    db_autoscaling_endpoint=LAKEBASE_CONFIG.autoscaling_endpoint,
    db_project=LAKEBASE_CONFIG.autoscaling_project,
    db_branch=LAKEBASE_CONFIG.autoscaling_branch,
    task_timeout_seconds=float(os.getenv("TASK_TIMEOUT_SECONDS", "3600")),
    poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "1.0")),
)

app = agent_server.app  # noqa: F841
try:
    setup_mlflow_git_based_version_tracking()
except Exception as exc:
    logger.warning("MLflow git version tracking unavailable (expected in App deployment): %s", exc)

_original_lifespan = app.router.lifespan_context

@asynccontextmanager
async def _lifespan(app):
    try:
        await run_lakebase_setup(LAKEBASE_CONFIG)
    except Exception as exc:
        logger.warning("Lakebase setup failed (memory features may be degraded): %s", exc)
    try:
        async with _original_lifespan(app):
            yield
    except Exception as exc:
        logger.warning("Long-running DB initialization failed: %s. Background mode disabled.", exc)
        yield

app.router.lifespan_context = _lifespan


def main():
    agent_server.run(app_import_string="agent_server.start_server:app")
