"""Obsidian vault connector for cognee — a ``dlt`` source that turns the markdown
notes of a local vault into memory, incrementally, with forget-on-delete.

The source is handed straight to :func:`cognee.remember`::

    import cognee
    from cognee_community_connector_obsidian import obsidian_source

    await cognee.remember(
        obsidian_source("~/Notes"),          # or OBSIDIAN_VAULT_PATH
        dataset_name="my_vault",
        primary_key="id",
        write_disposition="merge",            # incremental upsert by note path
    )

Design
------
* **Auth** — none. The vault is a directory of ``.md`` (and, tolerated, ``.mdx``)
  files; the connector only reads it.
* **Document mode** — the source declares ``cognee_document_source = "obsidian"``
  (the Notion and GitLab connectors do the same), so cognee turns every row into
  a text document built from the ``title`` and ``content`` columns and runs it
  through normal cognify, instead of the relational dlt-schema path.
* **Primary key** — the vault-relative POSIX path. Obsidian has no stable note
  id: the file *is* the note, and a rename is a delete plus a create. The README
  says so instead of pretending otherwise.
* **Incremental cursor** — ``(mtime, size)`` per note, kept in dlt's resource
  state. A note whose stat changed is re-read and re-emitted only when its
  ``sha256`` changed too: sync tools (iCloud, Syncthing) rewrite mtimes — even
  backwards — without touching content, and a touch must not re-cognify a note.
  Unchanged notes are not read at all. The dlt pipeline state lives in the
  destination, so re-running ``remember`` resumes where it left off.
* **Wikilinks are the graph** — ``[[note]]``, ``[[note|alias]]``,
  ``[[note#heading]]``, ``[[note#^block]]`` and ``![[embed]]`` are parsed per
  note and resolved the way Obsidian resolves them: a target containing ``/`` is
  a vault-root path, anything else matches a note by name or by one of its
  frontmatter ``aliases``, case-insensitively, shortest path first when several
  notes share a name. Links ship twice: as a structured ``links`` JSON column
  (target, alias, heading, embed flag, resolved path) for anyone who wants to
  materialise edges deterministically, and as a "Links to:" section appended to
  the document text so cognify sees the relations. When a note is created,
  removed or renamed, every unchanged note that links to that *name* is
  re-rendered, so dangling links resolve as soon as their target exists.
* **Frontmatter** — preserved verbatim as a ``frontmatter`` JSON column. The
  ``title``, ``tags``/``tag``, ``aliases``/``alias`` and ``url``/``permalink``
  properties are also lifted into their own columns (Obsidian's default
  property names; ``tag``/``alias`` are the pre-1.9 spellings).
* **Forget-on-delete** — a filesystem has no deletion feed, so the directory
  walk doubles as a sweep against the previous run's paths. Notes that vanished
  are emitted with the ``_deleted`` hard-delete marker; dlt removes those rows
  on ``merge`` and cognee's ``orphan_cleanup`` purges them from the graph, vector
  and relational stores. A walk that finds *no* notes where some were known is
  treated as a transient failure (unmounted volume, wrong path) and deletes
  nothing that run.
* **MDX** — ``.mdx`` notes are accepted; ``import``/``export`` lines and JSX
  components are stripped from the text, everything else is markdown.
* **Own pipeline state** — the source carries cognee's ``cognee_pipeline_scope``
  marker, so its dlt pipeline (and with it the per-note cursor and known-path
  set) is namespaced per dataset and vault. Without it every unscoped dlt
  source shares one pipeline name and another connector run in between would
  overwrite this one's state, and deletions would never be detected.
* **Robust to a bad file** — a note that cannot be read (permissions, vanished
  between walk and read), is empty, or is not text (NUL bytes) is skipped for
  the run with a log line: its last known version stays, nothing is tombstoned,
  the cursor does not advance, and it is retried next run.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR, PIPELINE_SCOPE_ATTR

logger = get_logger("obsidian_connector")

OBSIDIAN_SOURCE_NAME = "obsidian"
OBSIDIAN_TABLE_NAME = "obsidian_notes"

#: Note file suffixes. ``.mdx`` is tolerated, not promoted: Obsidian itself only
#: opens ``.md``, but vaults exported from doc sites carry MDX next to markdown.
NOTE_SUFFIXES: tuple[str, ...] = (".md", ".mdx")

#: Directories never descended into: Obsidian's own config and trash, VCS, JS deps.
DEFAULT_EXCLUDE_DIRS: frozenset[str] = frozenset({".obsidian", ".trash", ".git", "node_modules"})

#: File globs skipped by default (matched against the vault-relative path).
#: Syncthing writes ``name.sync-conflict-<date>.md`` copies next to live notes.
DEFAULT_EXCLUDE_GLOBS: tuple[str, ...] = ("*.sync-conflict-*",)

#: Embeds whose target has one of these extensions are attachments, not notes.
ATTACHMENT_SUFFIXES: frozenset[str] = frozenset(
    {
        ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".bmp", ".avif",
        ".mp3", ".wav", ".m4a", ".ogg", ".3gp", ".flac",
        ".mp4", ".webm", ".ogv", ".mov", ".mkv",
        ".pdf", ".canvas", ".base",
    }
)  # fmt: skip

#: Version of the per-resource state layout; bump on incompatible changes.
STATE_VERSION = 1

# ``[[target#heading|alias]]`` or ``![[embed]]``; the body may not contain brackets.
_WIKILINK_RE = re.compile(r"(!?)\[\[([^\[\]\n]+?)\]\]")
# Fenced and inline code: links inside code are text, not links.
_CODE_RE = re.compile(r"```.*?```|~~~.*?~~~|`[^`\n]*`", re.DOTALL)
# A YAML frontmatter block must start on the very first line.
_FRONTMATTER_RE = re.compile(r"\A---[ \t]*\r?\n(.*?)\r?\n---[ \t]*(?:\r?\n|\Z)", re.DOTALL)
_H1_RE = re.compile(r"^#[ \t]+(.+?)[ \t]*#*[ \t]*$", re.MULTILINE)
# Inline tags: ``#tag``, ``#nested/tag``, ``#tag-1``; not ``#1`` (digits only), not
# anchors inside URLs (``…/page#section``), not headings (``# Heading`` has a space).
_INLINE_TAG_RE = re.compile(r"(?<![\w/#&])#((?=[\w/-]*[A-Za-z_-])[\w/-]+)")
# MDX: ESM lines and JSX components (capitalised tag names), self-closing or paired.
_MDX_ESM_RE = re.compile(r"^(?:import|export)\s[^\n]*$", re.MULTILINE)
_MDX_COMPONENT_RE = re.compile(
    r"<([A-Z][A-Za-z0-9.]*)\b[^>]*?/>|<([A-Z][A-Za-z0-9.]*)\b[^>]*?>.*?</\2\s*>", re.DOTALL
)

_EXTRA_HINT = (
    "The Obsidian connector requires dlt and PyYAML: "
    'pip install "cognee-community-connector-obsidian".'
)


# ---------------------------------------------------------------------------
# Parsing (pure functions)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class WikiLink:
    """One ``[[...]]`` occurrence.

    ``target`` is the note name or path as written, without alias, heading or
    extension; it is ``""`` for same-note links such as ``[[#Heading]]``.
    ``heading`` keeps the part after ``#`` (``^id`` for block links).
    ``kind`` is ``"note"`` or ``"attachment"`` (by target extension).
    ``resolved`` is the vault-relative path of the target note, or ``None``.
    """

    target: str
    alias: str | None = None
    heading: str | None = None
    embed: bool = False
    kind: str = "note"
    resolved: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def split_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    """Return ``(frontmatter, body)``.

    Frontmatter is the YAML block between ``---`` lines at the very start of the
    note (Obsidian "Properties"). Malformed or non-mapping YAML degrades to
    ``({}, text)`` so the note is still ingested as plain text.
    """
    match = _FRONTMATTER_RE.match(text)
    if not match:
        return {}, text
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - depends on install
        raise ImportError(_EXTRA_HINT) from exc
    try:
        data = yaml.safe_load(match.group(1))
    except yaml.YAMLError:
        logger.warning("Obsidian: malformed frontmatter ignored; note ingested as plain text.")
        return {}, text
    if not isinstance(data, dict):
        return {}, text
    return data, text[match.end() :]


def parse_wikilinks(body: str) -> list[WikiLink]:
    """All wikilinks of a note body in order of appearance, code spans excluded.

    Handles ``[[a]]``, ``[[a|alias]]``, ``[[a#h]]``, ``[[a#^block]]``,
    ``[[a#h|alias]]``, ``[[#h]]`` and the ``!`` embed prefix. The ``.md``
    extension is dropped from note targets, as Obsidian allows either spelling.
    """
    links: list[WikiLink] = []
    for match in _WIKILINK_RE.finditer(_CODE_RE.sub("", body)):
        embed = match.group(1) == "!"
        inner = match.group(2)
        target, alias = inner, None
        if "|" in inner:
            target, alias = inner.split("|", 1)
            alias = alias.strip() or None
        heading = None
        if "#" in target:
            target, heading = target.split("#", 1)
            heading = heading.strip() or None
        target = target.strip().replace("\\", "/").strip("/")
        if alias is not None and embed and re.fullmatch(r"\d+(x\d+)?", alias):
            alias = None  # ``![[image.png|640x480]]`` is a size, not an alias
        kind = "attachment" if Path(target).suffix.lower() in ATTACHMENT_SUFFIXES else "note"
        if kind == "note" and target.lower().endswith(".md"):
            target = target[:-3]
        links.append(WikiLink(target=target, alias=alias, heading=heading, embed=embed, kind=kind))
    return links


def _as_str_list(value: Any) -> list[str]:
    """Normalise a frontmatter list-ish value (list, scalar, ``"a, b"``) to strings."""
    if value is None or value is False:
        return []
    if isinstance(value, str):
        parts = re.split(r"[,\s]+", value.strip()) if value.strip() else []
    elif isinstance(value, (list, tuple)):
        parts = [str(v) for v in value if v is not None]
    else:
        parts = [str(value)]
    seen: list[str] = []
    for part in parts:
        part = part.strip()
        if part and part not in seen:
            seen.append(part)
    return seen


def parse_tags(frontmatter: dict[str, Any], body: str) -> list[str]:
    """Frontmatter ``tags``/``tag`` plus inline ``#tags`` (outside code), deduplicated."""
    tags = [t.lstrip("#") for t in _as_str_list(frontmatter.get("tags", frontmatter.get("tag")))]
    for match in _INLINE_TAG_RE.finditer(_CODE_RE.sub("", body)):
        tags.append(match.group(1))
    out: list[str] = []
    for tag in tags:
        tag = tag.strip("/")
        if tag and tag not in out:
            out.append(tag)
    return out


def parse_aliases(frontmatter: dict[str, Any]) -> list[str]:
    """Frontmatter ``aliases`` (or pre-1.9 ``alias``) as a list of strings."""
    return _as_str_list(frontmatter.get("aliases", frontmatter.get("alias")))


def note_title(frontmatter: dict[str, Any], body: str, path: str) -> str:
    """Frontmatter ``title`` → first ``# H1`` → file name (Obsidian's own display name)."""
    title = frontmatter.get("title")
    if isinstance(title, str) and title.strip():
        return title.strip()
    match = _H1_RE.search(body)
    if match:
        return match.group(1).strip()
    return Path(path).stem


def note_url(frontmatter: dict[str, Any]) -> str:
    """``url`` or ``permalink`` property when it is a string, else ``""``."""
    for key in ("url", "permalink"):
        value = frontmatter.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def strip_mdx(body: str) -> str:
    """Remove ESM ``import``/``export`` lines and JSX components from an MDX body."""
    body = _MDX_ESM_RE.sub("", body)
    body = _MDX_COMPONENT_RE.sub("", body)
    return re.sub(r"\n{3,}", "\n\n", body)


@dataclass
class ParsedNote:
    path: str
    frontmatter: dict[str, Any]
    body: str
    title: str
    tags: list[str]
    aliases: list[str]
    url: str
    links: list[WikiLink]
    sha256: str
    size: int
    mtime: float

    @property
    def link_targets(self) -> list[str]:
        """Lower-cased resolution keys this note's links depend on."""
        return sorted({_norm(link.target) for link in self.links if link.target})


def parse_note(path: str, raw: bytes, mtime: float) -> ParsedNote:
    """Parse one note from its bytes. Pure: no filesystem access."""
    text = raw.decode("utf-8", errors="replace").replace("\r\n", "\n")
    frontmatter, body = split_frontmatter(text)
    if path.lower().endswith(".mdx"):
        body = strip_mdx(body)
    return ParsedNote(
        path=path,
        frontmatter=frontmatter,
        body=body.strip(),
        title=note_title(frontmatter, body, path),
        tags=parse_tags(frontmatter, body),
        aliases=parse_aliases(frontmatter),
        url=note_url(frontmatter),
        links=parse_wikilinks(body),
        sha256=hashlib.sha256(raw).hexdigest(),
        size=len(raw),
        mtime=mtime,
    )


# ---------------------------------------------------------------------------
# Link resolution
# ---------------------------------------------------------------------------
def _norm(name: str) -> str:
    # NFC: macOS stores file names decomposed (NFD) while a link typed in Obsidian is
    # composed (NFC); "Café" must be one key either way.
    return unicodedata.normalize("NFC", name).strip().lower()


class LinkIndex:
    """Resolve wikilink targets to vault paths the way Obsidian does.

    * ``[[folder/note]]`` — a path from the vault root, extension optional.
    * ``[[note]]`` — a note name anywhere in the vault; when several notes share
      the name the one with the shortest path wins (Obsidian's "shortest path
      when possible" rule), ties broken alphabetically for determinism.
    * ``[[Alias]]`` — a frontmatter alias, resolved after names.
    * Matching is case-insensitive.
    """

    def __init__(self, notes: Iterable[tuple[str, list[str]]]):
        self._by_path: dict[str, str] = {}
        self._by_name: dict[str, list[str]] = {}
        self._by_alias: dict[str, list[str]] = {}
        for path, aliases in notes:
            stem_path = _norm(_strip_suffix(path))
            self._by_path[stem_path] = path
            self._by_name.setdefault(_norm(Path(path).stem), []).append(path)
            for alias in aliases:
                self._by_alias.setdefault(_norm(alias), []).append(path)
        for candidates in (*self._by_name.values(), *self._by_alias.values()):
            candidates.sort(key=lambda p: (p.count("/"), p))

    def resolve(self, target: str) -> str | None:
        key = _norm(target)
        if not key:
            return None
        if "/" in key:
            hit = self._by_path.get(key) or self._by_path.get(_strip_suffix(key))
            if hit:
                return hit
            key = Path(key).name  # ``[[dir/name]]`` falls back to the name
        for table in (self._by_name, self._by_alias):
            candidates = table.get(key) or table.get(_strip_suffix(key))
            if candidates:
                return candidates[0]
        return None

    def resolve_all(self, links: Iterable[WikiLink]) -> list[WikiLink]:
        out: list[WikiLink] = []
        for link in links:
            if link.kind != "note" or not link.target:
                out.append(link)
            else:
                out.append(WikiLink(**{**link.as_dict(), "resolved": self.resolve(link.target)}))
        return out


def _strip_suffix(path: str) -> str:
    lower = path.lower()
    for suffix in NOTE_SUFFIXES:
        if lower.endswith(suffix):
            return path[: -len(suffix)]
    return path


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------
def render_content(note: ParsedNote, links: list[WikiLink], titles: dict[str, str]) -> str:
    """Document text: the body, then deterministic ``Links to:`` / ``Embeds:`` /
    ``Unresolved links:`` / ``Tags:`` lines so cognify sees the wikilink graph.

    Resolved targets are named by their *title* (what the target document is
    called in the graph); unresolved ones by the raw target. Pure function of
    the inputs, so an untouched note renders identically on every run.
    """
    sections: list[str] = [note.body] if note.body else []
    resolved_links: list[str] = []
    resolved_embeds: list[str] = []
    unresolved: list[str] = []
    for link in links:
        if link.kind != "note" or not link.target:
            continue
        if link.resolved:
            label = titles.get(link.resolved) or Path(link.resolved).stem
            bucket = resolved_embeds if link.embed else resolved_links
        else:
            label, bucket = link.target, unresolved
        if label not in bucket:
            bucket.append(label)
    lines: list[str] = []
    if resolved_links:
        lines.append("Links to: " + "; ".join(resolved_links))
    if resolved_embeds:
        lines.append("Embeds: " + "; ".join(resolved_embeds))
    if unresolved:
        lines.append("Unresolved links: " + "; ".join(unresolved))
    if note.tags:
        lines.append("Tags: " + ", ".join(note.tags))
    if lines:
        sections.append("\n".join(lines))
    return "\n\n".join(sections)


def note_row(note: ParsedNote, links: list[WikiLink], titles: dict[str, str]) -> dict[str, Any]:
    """Flatten a parsed note into the ``obsidian_notes`` row.

    Everything cognee's document path reads (``id``, ``title``, ``content``,
    ``url``) is a plain column; richer structure (``frontmatter``, ``tags``,
    ``aliases``, ``links``) is JSON text so it stays one row per note in the
    destination and is still queryable with the database's JSON functions.
    """
    parent = Path(note.path).parent.as_posix()
    return {
        "id": note.path,
        "title": note.title,
        "content": render_content(note, links, titles),
        "url": note.url,
        "path": note.path,
        "folder": "" if parent == "." else parent,
        "name": Path(note.path).stem,
        "extension": Path(note.path).suffix.lstrip(".").lower(),
        "modified_at": datetime.fromtimestamp(note.mtime, tz=UTC).isoformat(),
        "size_bytes": note.size,
        "sha256": note.sha256,
        "tags": json.dumps(note.tags, ensure_ascii=False),
        "aliases": json.dumps(note.aliases, ensure_ascii=False),
        "frontmatter": json.dumps(
            note.frontmatter, ensure_ascii=False, sort_keys=True, default=str
        ),
        "links": json.dumps([link.as_dict() for link in links], ensure_ascii=False),
        "link_count": sum(1 for link in links if link.kind == "note" and link.target),
        "unresolved_link_count": sum(
            1 for link in links if link.kind == "note" and link.target and not link.resolved
        ),
        # Hard-delete marker (always False for live notes). Vanished notes are
        # emitted separately with _deleted=True.
        "_deleted": False,
    }


def deleted_row(path: str) -> dict[str, Any]:
    """Minimal row that instructs dlt to hard-delete a note by path."""
    return {"id": path, "_deleted": True}


# ---------------------------------------------------------------------------
# Vault scan + sync (pure given a root and a state dict — unit-testable)
# ---------------------------------------------------------------------------
@dataclass
class VaultScan:
    """What the walk found: ``path → (mtime, size)``."""

    stats: dict[str, tuple[float, int]] = field(default_factory=dict)


def scan_vault(
    root: Path,
    *,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
    exclude_dirs: Iterable[str] | None = None,
    suffixes: Iterable[str] = NOTE_SUFFIXES,
) -> VaultScan:
    """Walk ``root`` and stat every note. Deterministic order, no file reads.

    Excluded and dot-directories are pruned before descent (``.git`` is never
    walked). Symbolic links are not followed: a linked directory could point
    outside the vault or loop, and a linked file would be deleted twice.
    """
    include = tuple(include or ())
    exclude = tuple(exclude if exclude is not None else DEFAULT_EXCLUDE_GLOBS)
    skipped = frozenset(exclude_dirs if exclude_dirs is not None else DEFAULT_EXCLUDE_DIRS)
    suffixes = tuple(s.lower() for s in suffixes)
    scan = VaultScan()
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames[:] = sorted(d for d in dirnames if d not in skipped and not d.startswith("."))
        rel_dir = Path(dirpath).relative_to(root)
        for name in sorted(filenames):
            path = Path(dirpath) / name
            if path.suffix.lower() not in suffixes or path.is_symlink() or not path.is_file():
                continue
            rel = rel_dir / name
            rel_posix = rel.as_posix()
            if include and not any(fnmatchcase(rel_posix, pat) for pat in include):
                continue
            if any(fnmatchcase(rel_posix, pat) or fnmatchcase(rel.name, pat) for pat in exclude):
                continue
            stat = path.stat()
            scan.stats[rel_posix] = (stat.st_mtime, stat.st_size)
    return scan


def _read_note(root: Path, path: str) -> bytes | None:
    """Bytes of a note, or ``None`` when it must be skipped this run.

    ``None`` for an unreadable file (permissions, vanished between walk and
    read), an empty file, or one containing NUL bytes (not text). The caller
    keeps the last known entry untouched, so nothing is tombstoned and the
    cursor does not advance; the note is simply retried next run.
    """
    try:
        raw = (root / path).read_bytes()
    except OSError as exc:
        logger.warning("Obsidian: cannot read %s (%s); keeping its last known version.", path, exc)
        return None
    if not raw:
        logger.debug("Obsidian: %s is empty; skipped this run.", path)
        return None
    if b"\x00" in raw:
        logger.warning("Obsidian: %s contains NUL bytes (not text); skipped this run.", path)
        return None
    return raw


def _resolution_keys(path: str, aliases: Iterable[str]) -> set[str]:
    """Every lower-cased target string that could resolve to this note."""
    stem_path = _strip_suffix(path)
    keys = {_norm(stem_path), _norm(Path(path).stem)}
    keys.update(_norm(alias) for alias in aliases)
    return keys


def sync_vault(root: Path, state: dict[str, Any], **scan_options: Any) -> Iterator[dict[str, Any]]:
    """Yield rows for notes that changed since the last run, plus delete markers.

    ``state`` is dlt's per-resource state (any mutable mapping works):
    ``state["notes"][path] = {mtime, size, sha256, title, aliases, targets,
    row_hash}``. The walk drives three decisions — which notes to read (stat
    changed or new), which unchanged notes to re-render (they link to a name
    whose resolution changed), and which notes vanished (tombstones).
    """
    known: dict[str, dict[str, Any]] = dict(state.get("notes") or {})
    scan = scan_vault(root, **scan_options)
    current = scan.stats

    # An empty walk while notes were previously known almost always means an
    # unmounted volume or a wrong path — not a wiped vault. Treating it as "all
    # deleted" would purge the dataset and forget the known set, making the loss
    # permanent. Skip deletion and leave the state untouched this run.
    if known and not current:
        logger.warning(
            "Obsidian: walk of %s found 0 notes but %d were known; skipping deletion this run.",
            root,
            len(known),
        )
        return

    # Pass 1 — read notes that are new or whose stat changed; keep the rest.
    # Memory ceiling: the bodies of the notes parsed this run (changed + re-rendered)
    # are held until pass 3 so links can be resolved against the final index. That is
    # the size of the *changed* notes, which on the first sync is the whole vault
    # (about 1.4x its bytes with the parsed structures). Vaults are text; re-reading
    # in pass 3 instead would double the reads of every changed note on every run.
    parsed: dict[str, ParsedNote] = {}
    entries: dict[str, dict[str, Any]] = {}
    changed_keys: set[str] = set()
    for path, (mtime, size) in current.items():
        old = known.get(path)
        if old and (old.get("mtime"), old.get("size")) == (mtime, size):
            entries[path] = dict(old)
            continue
        raw = _read_note(root, path)
        if raw is None:
            if old:
                entries[path] = dict(old)  # keep the last known version; retry next run
            continue
        if old and old.get("sha256") == hashlib.sha256(raw).hexdigest():
            # Touched, not edited (sync tools rewrite mtimes): refresh the cursor only.
            entries[path] = {**old, "mtime": mtime, "size": size}
            continue
        note = parse_note(path, raw, mtime)
        parsed[path] = note
        entries[path] = {
            "mtime": mtime,
            "size": size,
            "sha256": note.sha256,
            "title": note.title,
            "aliases": note.aliases,
            "targets": note.link_targets,
            "row_hash": "",
        }
        # Which *other* notes must re-render? Only those whose link text or resolution
        # can change: a new name appeared, a title changed (the "Links to:" line names
        # targets by title), or an alias was added or removed. A body-only edit of an
        # existing note changes nothing for its neighbours, so they are not re-read.
        old_aliases = set(old.get("aliases") or []) if old else set()
        if old is None:
            changed_keys |= _resolution_keys(path, note.aliases)
        elif old.get("title") != note.title:
            changed_keys |= _resolution_keys(path, old_aliases | set(note.aliases))
        else:
            changed_keys |= {_norm(alias) for alias in old_aliases ^ set(note.aliases)}
    deleted = sorted(set(known) - set(current))
    for path in deleted:
        changed_keys |= _resolution_keys(path, known[path].get("aliases") or [])

    # Pass 2 — unchanged notes whose links point at a name whose resolution may
    # have changed are re-rendered (a dangling link now resolves, a target was
    # renamed or deleted, a title changed).
    for path, entry in entries.items():
        if path in parsed:
            continue
        if changed_keys and set(entry.get("targets") or ()) & changed_keys:
            raw = _read_note(root, path)
            if raw is not None:
                parsed[path] = parse_note(path, raw, entry["mtime"])

    # Pass 3 — resolve against the whole current vault and emit rows whose
    # rendered form differs from what was loaded last time.
    index = LinkIndex((path, entry.get("aliases") or []) for path, entry in entries.items())
    titles = {path: entry.get("title") or Path(path).stem for path, entry in entries.items()}
    emitted = 0
    for path in sorted(parsed):
        note = parsed[path]
        row = note_row(note, index.resolve_all(note.links), titles)
        # The hash decides "did anything cognee sees change?". mtime and size are the
        # cursor, not content: a touched note re-rendered because of a neighbour must
        # not be re-emitted when its text, links and metadata are identical.
        hashed = {k: v for k, v in row.items() if k not in ("modified_at", "size_bytes")}
        row_hash = hashlib.sha256(
            json.dumps(hashed, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        if entries[path].get("row_hash") == row_hash:
            continue
        entries[path]["row_hash"] = row_hash
        emitted += 1
        yield row

    for path in deleted:
        yield deleted_row(path)

    state["notes"] = entries
    state["version"] = STATE_VERSION
    logger.info(
        "Obsidian: %d notes in vault, %d emitted, %d deleted.", len(current), emitted, len(deleted)
    )


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def obsidian_source(
    vault_path: str | os.PathLike[str] | None = None,
    *,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
    exclude_dirs: Iterable[str] | None = None,
    suffixes: Iterable[str] = NOTE_SUFFIXES,
):
    """Return a ``dlt`` source that yields the notes of an Obsidian vault.

    Args:
        vault_path: The vault directory. Falls back to ``OBSIDIAN_VAULT_PATH``.
        include: Optional glob patterns on the vault-relative path (``"projects/*"``,
            ``"**/*.md"``); when given, only matching notes are ingested.
        exclude: Glob patterns to skip (default: Syncthing conflict copies).
        exclude_dirs: Directory names never descended into (default: ``.obsidian``,
            ``.trash``, ``.git``, ``node_modules``). Dot-directories are always skipped.
        suffixes: Note file suffixes (default ``.md`` and ``.mdx``).

    Returns:
        A ``dlt`` source named ``obsidian`` with one resource, ``obsidian_notes``,
        configured with ``primary_key="id"``, ``write_disposition="merge"`` and a
        ``_deleted`` hard-delete column, tagged as a cognee document source with
        its own pipeline scope.
    """
    try:
        import dlt
    except ImportError as exc:  # pragma: no cover - depends on install
        raise ImportError(_EXTRA_HINT) from exc

    raw_path = vault_path or os.environ.get("OBSIDIAN_VAULT_PATH")
    if not raw_path:
        raise ValueError("obsidian_source requires vault_path= or OBSIDIAN_VAULT_PATH.")
    root = Path(raw_path).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"Obsidian vault not found (not a directory): {raw_path}")
    scan_options = {
        "include": list(include) if include else None,
        "exclude": list(exclude) if exclude is not None else None,
        "exclude_dirs": list(exclude_dirs) if exclude_dirs is not None else None,
        "suffixes": tuple(suffixes),
    }

    @dlt.resource(
        name=OBSIDIAN_TABLE_NAME,
        primary_key="id",
        write_disposition="merge",
        # _deleted is a boolean hard-delete marker: rows where it is True are
        # removed from the dlt destination on merge, which propagates the
        # deletion through cognee's orphan_cleanup.
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def obsidian_notes():
        yield from sync_vault(root, dlt.current.resource_state(), **scan_options)

    @dlt.source(name=OBSIDIAN_SOURCE_NAME)
    def _obsidian():
        return obsidian_notes

    source = _obsidian()
    # Opt into the document ingestion path (note → text document → cognify).
    # resolve_dlt_sources reads this marker; it never imports this connector.
    setattr(source, DOCUMENT_SOURCE_ATTR, OBSIDIAN_SOURCE_NAME)
    # Own dlt pipeline per (dataset, vault): cognee derives the pipeline name from this
    # scope, so the resource state (cursor + known paths) cannot be overwritten by
    # another dlt source that runs in between (see module docstring).
    setattr(source, PIPELINE_SCOPE_ATTR, f"{OBSIDIAN_SOURCE_NAME}:{root.as_posix()}")
    return source
