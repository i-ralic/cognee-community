"""Obsidian connector demo — turn a vault of markdown notes into memory.

Builds a small demo vault in a temporary directory (so this runs with no existing
notes), syncs it into cognee, then edits one note, adds one and deletes one and
syncs again, so you can see incremental sync and forget-on-delete at work.

Point ``obsidian_source`` at your own vault to ingest real notes::

    source = obsidian_source("~/Notes")            # or OBSIDIAN_VAULT_PATH
    source = obsidian_source("~/Notes", include=["projects/*"])

Needs an ``LLM_API_KEY`` like any other cognee run (or run inside a cognee image
configured for GLiNER + fastembed, which needs none).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

import cognee

from cognee_community_connector_obsidian import obsidian_source

DATASET = "obsidian_demo"

REMEMBER_KWARGS = {"primary_key": "id", "write_disposition": "merge"}

NOTES = {
    "Agents.md": "---\ntitle: Agents\ntags: [ai, memory]\n---\n"
    "Agents remember things across sessions via [[Memory]].\n",
    "Memory.md": "---\ntitle: Memory\naliases: [Long-term memory]\n---\n"
    "Memory stores [[Agents|agent]] context and is built by [[Cognify]].\n",
    "Cognify.md": "# Cognify\n\nCognify turns documents into a knowledge graph. #pipeline\n",
}


async def main() -> None:
    if not os.environ.get("LLM_API_KEY") and not os.environ.get("GRAPH_DATABASE_PROVIDER"):
        print("Set LLM_API_KEY (or configure a key-less cognee) to run this example.")
        return

    with tempfile.TemporaryDirectory(prefix="obsidian_demo_") as vault:
        root = Path(vault)
        for name, text in NOTES.items():
            (root / name).write_text(text, encoding="utf-8")

        print("Sync 1: three notes ...")
        await cognee.remember(obsidian_source(root), dataset_name=DATASET, **REMEMBER_KWARGS)

        (root / "Cognify.md").write_text(
            "# Cognify\n\nCognify turns documents into a graph with [[Long-term memory]].\n",
            encoding="utf-8",
        )
        (root / "Search.md").write_text("Search reads the graph [[Cognify]] built.\n")
        (root / "Agents.md").unlink()

        print("Sync 2: one edit, one add, one delete ...")
        await cognee.remember(obsidian_source(root), dataset_name=DATASET, **REMEMBER_KWARGS)

    answer = await cognee.search(
        query_text="What builds memory, and what reads it?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET],
    )
    print("\nSearch result:\n", answer)
    print("\nDeleted 'Agents' is gone from memory; 'Search' and the edited 'Cognify' are in.")


if __name__ == "__main__":
    asyncio.run(main())
