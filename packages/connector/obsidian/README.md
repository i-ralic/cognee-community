# cognee-community-connector-obsidian

An Obsidian vault data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync a local vault of markdown notes into memory — "ask my notes". Implements
[cognee#4725](https://github.com/topoteretes/cognee/issues/4725).

It exposes a `dlt` source you hand to `cognee.remember(...)` / `cognee.add(...)`. Notes are
ingested as **normal documents** (they flow through cognee's cognify entity-extraction
pipeline, not the deterministic dlt-row path), via cognee's document-mode marker — the same
routing the Notion connector uses.

| | |
|---|---|
| Auth | none — the vault is a directory on disk; read-only |
| Identity | vault-relative path (`primary_key="id"`); a rename is a delete + a create |
| Incremental | `(mtime, size)` cursor per note with a `sha256` guard, in dlt resource state |
| Forget-on-delete | directory sweep → `_deleted` hard-delete rows → `merge` → `orphan_cleanup` |
| Wikilinks | parsed and resolved like Obsidian; `links` JSON column + "Links to:" text |
| Frontmatter | preserved as `frontmatter` JSON; `title`, `tags`, `aliases`, `url` lifted out |
| MDX | `.mdx` tolerated: ESM lines and JSX components stripped |

## Install

```bash
uv pip install cognee-community-connector-obsidian
# or, from this monorepo:
cd packages/connector/obsidian && uv sync --all-extras
```

Requires cognee ≥ 1.6 (document-mode dlt sources).

## Usage

```python
import cognee
from cognee_community_connector_obsidian import obsidian_source

await cognee.remember(
    obsidian_source("~/Notes"),  # or set OBSIDIAN_VAULT_PATH
    dataset_name="notes",
    primary_key="id",
    write_disposition="merge",  # incremental upsert by note path
    max_rows_per_table=0,  # 0 = no row cap, so orphan cleanup sees the whole vault
)

answer = await cognee.search(
    query_text="What do my notes say about project Atlas?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["notes"],
)
```

Scope what you ingest with `include=["projects/*", "daily/2026-*"]` (globs on the
vault-relative path) and `exclude=[...]`; `.obsidian/`, `.trash/`, `.git/`, `node_modules/`,
any dot-directory and Syncthing `*.sync-conflict-*` copies are skipped by default. See
`examples/example.py` for the full flow, including an edit and a delete.

Run it again whenever you like: the second run reads only the notes whose stat changed,
emits only those whose content changed, and tombstones the ones that vanished.

## What one note becomes

One row in the `obsidian_notes` table, one text document in cognee:

| column | content |
|---|---|
| `id`, `path`, `folder`, `name`, `extension` | identity: vault-relative POSIX path and its parts |
| `title` | frontmatter `title` → first `# H1` → file name |
| `content` | body (frontmatter stripped), then `Links to:` / `Embeds:` / `Unresolved links:` / `Tags:` lines |
| `url` | frontmatter `url` or `permalink`, if any |
| `modified_at`, `size_bytes`, `sha256` | the cursor that decided this row was (re)emitted |
| `tags`, `aliases` | JSON lists (frontmatter + inline `#tags`) |
| `frontmatter` | the whole properties block as JSON, untouched |
| `links` | JSON list of `{target, alias, heading, embed, kind, resolved}` |
| `link_count`, `unresolved_link_count` | note links only (attachments excluded) |
| `_deleted` | hard-delete marker; `True` only on tombstone rows |

cognee's document path reads `id`, `title`, `content` and `url`; the other columns stay in
the dlt destination (your relational store) where they are queryable — for example to
materialise `links_to` edges between note documents deterministically from `links`.

## How wikilinks are handled

Obsidian's link syntax, all forms: `[[note]]`, `[[note|alias]]`, `[[note#heading]]`,
`[[note#^block]]`, `[[note#heading|alias]]`, `[[folder/note]]`, `[[#heading]]` (same note),
`![[embed]]`, `![[image.png|640x480]]`. Links inside code spans and fences are ignored.

Resolution follows Obsidian: a target with a `/` is a path from the vault root; anything
else matches a note by file name (extension optional) or by a frontmatter alias,
case-insensitively; when several notes share a name the shortest path wins. Unresolved
links are kept (as `resolved: null` and an `Unresolved links:` line) so the graph can still
show what the author meant.

The `Links to:` line names resolved targets by their **title**, which is the name the target
document carries in cognee's graph — so entity extraction has the best chance of connecting
the two documents. When a note is created, deleted or renamed, or its title or aliases
change, every unchanged note that links to that name is re-rendered and re-emitted on the
same run, so dangling links resolve as soon as their target appears.

## How incremental sync and forget-on-delete work

1. **Walk** the vault (no reads): every note's `(mtime, size)`.
2. **Read** notes that are new or whose stat changed. If the `sha256` is unchanged the
   cursor is refreshed and nothing is emitted (sync tools rewrite mtimes, even backwards).
3. **Re-render** unchanged notes whose links point at a name whose resolution changed.
4. **Resolve** every link against the current vault and emit rows whose rendered form
   differs from the last load.
5. **Tombstone** paths that were known last run and are gone now: `{"id": path,
   "_deleted": true}`. dlt removes the row on `merge`; cognee's `orphan_cleanup` deletes
   the document, its chunks, graph nodes and vectors.

A walk that finds **zero** notes where some were known is treated as an unmounted volume
or a wrong path: nothing is deleted that run and the state is left untouched.

State lives in dlt's per-resource state (persisted with the pipeline in your destination),
so `remember` resumes where it left off across processes and containers.

## Limitations

- Obsidian has no stable note id; a moved or renamed note is forgotten and re-learned.
- Only `[[wikilinks]]` are parsed. Standard `[text](note.md)` markdown links are left as
  text (Obsidian can write either; the default is wikilinks).
- Embedded attachments (`![[image.png]]`) are recorded in `links` with
  `kind: "attachment"` but their bytes are not ingested.
- Block references (`#^id`) are kept as the `heading` field; blocks are not resolved.

## Development

```bash
cd packages/connector/obsidian
uv sync --all-extras
uv run pytest -q           # 40 tests, tmp_path vaults, no network, no LLM, no model
uv run ruff check . && uv run ruff format --check .
```
