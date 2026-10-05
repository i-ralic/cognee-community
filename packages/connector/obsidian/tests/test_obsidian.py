"""Tests for the Obsidian vault connector.

Everything runs on vaults written into ``tmp_path``: no real vault, no network,
no LLM key, no model download. Two layers:

* pure parsing / resolution / rendering functions;
* the sync algorithm against a state dict, plus one real ``dlt`` pipeline into a
  temporary SQLite destination to show that the ``_deleted`` marker really acts
  on ``merge`` (forget-on-delete end to end).
"""

from __future__ import annotations

import json
import os
import unicodedata
from pathlib import Path

import pytest
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    PIPELINE_SCOPE_ATTR,
    pipeline_name_for_source,
)

from cognee_community_connector_obsidian.obsidian import (
    OBSIDIAN_SOURCE_NAME,
    OBSIDIAN_TABLE_NAME,
    LinkIndex,
    WikiLink,
    note_row,
    note_title,
    obsidian_source,
    parse_aliases,
    parse_note,
    parse_tags,
    parse_wikilinks,
    render_content,
    scan_vault,
    split_frontmatter,
    strip_mdx,
    sync_vault,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def write_vault(root: Path, files: dict[str, str | bytes]) -> Path:
    """Write ``{relative path: text}`` under ``root`` and return ``root``."""
    for rel, content in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            target.write_bytes(content)
        else:
            target.write_text(content, encoding="utf-8")
    return root


def run_sync(root: Path, state: dict) -> tuple[dict[str, dict], list[str]]:
    """Run one sync; return ``({path: row}, [deleted paths])``."""
    rows, deleted = {}, []
    for row in sync_vault(root, state):
        if row.get("_deleted"):
            deleted.append(row["id"])
        else:
            rows[row["id"]] = row
    return rows, deleted


def bump_mtime(path: Path, seconds: float) -> None:
    stat = path.stat()
    os.utime(path, (stat.st_atime, stat.st_mtime + seconds))


# ---------------------------------------------------------------------------
# Frontmatter
# ---------------------------------------------------------------------------


def test_frontmatter_becomes_metadata_and_is_stripped_from_body():
    meta, body = split_frontmatter("---\ntitle: Dog\ntags: [a, b]\n---\n# Dog\nBody\n")
    assert meta == {"title": "Dog", "tags": ["a", "b"]}
    assert body == "# Dog\nBody\n"


def test_frontmatter_requires_line_one_and_tolerates_crlf():
    assert split_frontmatter("\n---\ntitle: x\n---\nbody")[0] == {}
    meta, body = split_frontmatter("---\r\ntitle: x\r\n---\r\nbody")
    assert meta == {"title": "x"} and body == "body"


@pytest.mark.parametrize("text", ["---\ntitle: [unclosed\n---\nbody", "---\n- a\n- b\n---\nbody"])
def test_malformed_or_non_mapping_frontmatter_keeps_note_as_plain_text(text):
    assert split_frontmatter(text) == ({}, text)


def test_title_falls_back_from_frontmatter_to_h1_to_file_name():
    assert note_title({"title": " Dog "}, "# Cat", "a/b.md") == "Dog"
    assert note_title({}, "intro\n\n# Cat #\n", "a/b.md") == "Cat"
    assert note_title({"title": ""}, "## not an h1", "a/My Note.md") == "My Note"


def test_tags_merge_frontmatter_and_inline_forms():
    body = "Text #inline and #nested/tag, #tag-1 but not #2024 nor x.io/page#frag `#code`\n# H\n"
    assert parse_tags({"tags": "fm1, #fm2"}, body) == [
        "fm1",
        "fm2",
        "inline",
        "nested/tag",
        "tag-1",
    ]
    assert parse_tags({"tag": ["old"]}, "") == ["old"]
    assert parse_tags({}, "") == []


def test_aliases_accept_list_and_legacy_scalar():
    assert parse_aliases({"aliases": ["Doggo", "Woofer"]}) == ["Doggo", "Woofer"]
    assert parse_aliases({"alias": "Pup"}) == ["Pup"]
    assert parse_aliases({}) == []


# ---------------------------------------------------------------------------
# Wikilinks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("[[a]]", WikiLink("a")),
        ("[[a|alias]]", WikiLink("a", alias="alias")),
        ("[[a#h]]", WikiLink("a", heading="h")),
        ("[[a#^blk]]", WikiLink("a", heading="^blk")),
        ("[[a#h|alias]]", WikiLink("a", alias="alias", heading="h")),
        ("![[a]]", WikiLink("a", embed=True)),
        ("[[#h]]", WikiLink("", heading="h")),
        ("[[dir/Note.md]]", WikiLink("dir/Note")),
        ("![[img.png|640x480]]", WikiLink("img.png", embed=True, kind="attachment")),
        (
            "![[doc.pdf#page=3]]",
            WikiLink("doc.pdf", heading="page=3", embed=True, kind="attachment"),
        ),
    ],
)
def test_wikilink_forms(text, expected):
    assert parse_wikilinks(text) == [expected]


def test_wikilinks_inside_code_are_not_links_and_order_is_kept():
    body = "see [[b]] then `[[not]]`\n```\n[[nor this]]\n```\nand [[a]] and [[b]] again"
    assert [link.target for link in parse_wikilinks(body)] == ["b", "a", "b"]


def test_link_index_resolves_like_obsidian():
    index = LinkIndex(
        [
            ("Dog.md", ["Doggo"]),
            ("pets/Cat.md", []),
            ("zoo/Cat.md", []),
            ("deep/er/Fish.md", []),
            ("notes/Project Plan.md", ["plan"]),
        ]
    )
    assert index.resolve("Dog") == "Dog.md"
    assert index.resolve("dog.md") == "Dog.md"  # case-insensitive, extension optional
    assert index.resolve("Doggo") == "Dog.md"  # alias
    assert index.resolve("Cat") == "pets/Cat.md"  # shortest path, then alphabetical
    assert index.resolve("zoo/Cat") == "zoo/Cat.md"  # path link picks the exact note
    assert index.resolve("ZOO/cat.md") == "zoo/Cat.md"
    assert index.resolve("elsewhere/Fish") == "deep/er/Fish.md"  # wrong folder → by name
    assert index.resolve("project plan") == "notes/Project Plan.md"
    assert index.resolve("PLAN") == "notes/Project Plan.md"
    assert index.resolve("Missing") is None
    assert index.resolve("") is None


def test_resolve_all_leaves_attachments_and_self_links_alone():
    index = LinkIndex([("A.md", [])])
    links = index.resolve_all(parse_wikilinks("[[A]] ![[x.png]] [[#top]] [[B]]"))
    assert [link.resolved for link in links] == ["A.md", None, None, None]
    assert links[1].kind == "attachment" and links[2].target == ""


# ---------------------------------------------------------------------------
# Rendering / rows
# ---------------------------------------------------------------------------


def _note(path: str, text: str, mtime: float = 1_700_000_000.0):
    return parse_note(path, text.encode("utf-8"), mtime)


def test_render_content_appends_deterministic_link_sections():
    note = _note("a.md", "---\ntags: [t1]\n---\nBody [[b]] and [[b|B!]] ![[c]] [[gone]] #t2")
    index = LinkIndex([("a.md", []), ("b.md", []), ("c.md", [])])
    titles = {"b.md": "Bee", "c.md": "Sea"}
    content = render_content(note, index.resolve_all(note.links), titles)
    assert content == (
        "Body [[b]] and [[b|B!]] ![[c]] [[gone]] #t2\n\n"
        "Links to: Bee\nEmbeds: Sea\nUnresolved links: gone\nTags: t1, t2"
    )
    assert content == render_content(note, index.resolve_all(note.links), titles)


def test_note_row_has_document_columns_and_json_structure():
    note = _note(
        "folder/Note.md",
        "---\ntitle: T\nurl: https://x/y\naliases: [N]\ncustom: {k: 1}\n---\nHi [[Other#h|o]]",
    )
    links = LinkIndex([("folder/Note.md", ["N"]), ("Other.md", [])]).resolve_all(note.links)
    row = note_row(note, links, {"Other.md": "Other"})
    assert row["id"] == row["path"] == "folder/Note.md"
    assert (row["title"], row["url"], row["folder"], row["name"]) == (
        "T",
        "https://x/y",
        "folder",
        "Note",
    )
    assert row["extension"] == "md" and row["_deleted"] is False
    assert row["modified_at"] == "2023-11-14T22:13:20+00:00"
    assert json.loads(row["frontmatter"])["custom"] == {"k": 1}
    assert json.loads(row["aliases"]) == ["N"]
    assert json.loads(row["links"]) == [
        {"target": "Other", "alias": "o", "heading": "h", "embed": False, "kind": "note",
         "resolved": "Other.md"}
    ]  # fmt: skip
    assert (row["link_count"], row["unresolved_link_count"]) == (1, 0)
    assert row["content"].endswith("Links to: Other")


def test_mdx_import_and_components_are_stripped_but_markdown_stays():
    body = (
        'import V from "@c/V.svelte";\n\n# Title\n\n<V src="u" />\n\n'
        "Text <Tabs>\n<Tab>x</Tab>\n</Tabs> end\n"
    )
    assert strip_mdx(body) == "\n\n# Title\n\nText  end\n"
    note = _note("n.mdx", "---\ntitle: M\n---\n" + body)
    assert "import" not in note.body and "<V" not in note.body and "Text" in note.body


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------


def test_scan_skips_vault_config_trash_dot_dirs_conflicts_and_non_notes(tmp_path):
    root = write_vault(
        tmp_path,
        {
            "a.md": "a",
            "sub/b.mdx": "b",
            "sub/c.txt": "c",
            "sub/image.png": b"\x89PNG",
            ".obsidian/workspace.md": "x",
            ".trash/old.md": "x",
            ".stversions/v.md": "x",
            "a.sync-conflict-20260101-120000-ABCDEF.md": "x",
            "node_modules/pkg/readme.md": "x",
        },
    )
    assert sorted(scan_vault(root).stats) == ["a.md", "sub/b.mdx"]
    assert sorted(scan_vault(root, include=["sub/*"]).stats) == ["sub/b.mdx"]
    assert sorted(scan_vault(root, suffixes=(".md",)).stats) == ["a.md"]
    assert "a.sync-conflict-20260101-120000-ABCDEF.md" in scan_vault(root, exclude=[]).stats


def test_scan_does_not_follow_symlinks_and_never_walks_excluded_dirs(tmp_path, monkeypatch):
    outside = write_vault(tmp_path / "outside", {"secret.md": "x", "deep/more.md": "y"})
    root = write_vault(tmp_path / "vault", {"a.md": "a", ".git/objects/blob.md": "x"})
    (root / "linked_dir").symlink_to(outside, target_is_directory=True)
    (root / "linked_note.md").symlink_to(outside / "secret.md")
    assert sorted(scan_vault(root).stats) == ["a.md"]
    # .git is pruned before descent, not filtered after walking it.
    walked: list[str] = []
    real_walk = os.walk

    def spy(top, **kw):
        for dirpath, dirnames, filenames in real_walk(top, **kw):
            walked.append(str(dirpath))
            yield dirpath, dirnames, filenames

    monkeypatch.setattr(os, "walk", spy)
    scan_vault(root)
    assert walked and not any(".git" in w or "outside" in w for w in walked)


# ---------------------------------------------------------------------------
# Sync algorithm (state dict stands in for dlt resource state)
# ---------------------------------------------------------------------------

VAULT = {
    "Dog.md": "---\ntitle: Dog\naliases: [Doggo]\n---\nDogs chase [[Cat]]s. See [[Vet]].\n",
    "pets/Cat.md": "# Cat\n\nCats ignore [[Doggo]].\n",
    "Vet.md": "Vets treat ![[Dog]] and [[pets/Cat|cats]].\n",
}


def test_first_sync_emits_every_note_with_resolved_links_and_records_state(tmp_path):
    root = write_vault(tmp_path, VAULT)
    state: dict = {}
    rows, deleted = run_sync(root, state)
    assert sorted(rows) == ["Dog.md", "Vet.md", "pets/Cat.md"] and deleted == []
    assert rows["Dog.md"]["content"].endswith("Links to: Cat; Vet")
    assert rows["pets/Cat.md"]["content"].endswith("Links to: Dog")  # alias → title
    assert rows["Vet.md"]["content"].endswith("Links to: Cat\nEmbeds: Dog")
    assert state["version"] == 1 and sorted(state["notes"]) == sorted(rows)
    entry = state["notes"]["Dog.md"]
    assert entry["aliases"] == ["Doggo"] and entry["targets"] == ["cat", "vet"]
    assert entry["sha256"] and entry["row_hash"]


def test_second_sync_without_changes_is_a_noop(tmp_path):
    root = write_vault(tmp_path, VAULT)
    state: dict = {}
    run_sync(root, state)
    before = json.dumps(state, sort_keys=True)
    assert run_sync(root, state) == ({}, [])
    assert json.dumps(state, sort_keys=True) == before


def test_edited_note_is_re_emitted_alone(tmp_path):
    root = write_vault(tmp_path, VAULT)
    state: dict = {}
    run_sync(root, state)
    (root / "Vet.md").write_text("Vets treat [[Dog]] only.\n", encoding="utf-8")
    bump_mtime(root / "Vet.md", 10)
    rows, deleted = run_sync(root, state)
    assert list(rows) == ["Vet.md"] and deleted == []
    assert rows["Vet.md"]["content"] == "Vets treat [[Dog]] only.\n\nLinks to: Dog"


def test_touched_note_with_same_content_refreshes_cursor_without_emitting(tmp_path):
    root = write_vault(tmp_path, VAULT)
    state: dict = {}
    run_sync(root, state)
    bump_mtime(root / "Vet.md", 3600)
    assert run_sync(root, state) == ({}, [])
    assert state["notes"]["Vet.md"]["mtime"] == (root / "Vet.md").stat().st_mtime


def test_mtime_moved_backwards_with_changed_content_is_still_caught(tmp_path):
    root = write_vault(tmp_path, VAULT)
    state: dict = {}
    run_sync(root, state)
    (root / "Vet.md").write_text("Vets treat [[Dog]].\n", encoding="utf-8")
    bump_mtime(root / "Vet.md", -86_400)
    rows, _ = run_sync(root, state)
    assert list(rows) == ["Vet.md"]


def test_new_note_re_renders_unchanged_neighbours_whose_links_now_resolve(tmp_path):
    root = write_vault(tmp_path, {**VAULT, "Dog.md": "Dogs visit the [[Groomer]].\n"})
    state: dict = {}
    rows, _ = run_sync(root, state)
    assert rows["Dog.md"]["content"].endswith("Unresolved links: Groomer")
    write_vault(root, {"Groomer.md": "---\ntitle: The Groomer\n---\nBrushes.\n"})
    rows, deleted = run_sync(root, state)
    assert sorted(rows) == ["Dog.md", "Groomer.md"] and deleted == []
    assert rows["Dog.md"]["content"].endswith("Links to: The Groomer")
    assert rows["Dog.md"]["unresolved_link_count"] == 0


def test_rename_is_delete_plus_add_and_linking_notes_follow(tmp_path):
    root = write_vault(tmp_path, VAULT)
    state: dict = {}
    run_sync(root, state)
    (root / "Vet.md").rename(root / "Veterinarian.md")
    rows, deleted = run_sync(root, state)
    assert deleted == ["Vet.md"]
    # The renamed file is new; Dog.md linked to [[Vet]], which no longer resolves.
    assert sorted(rows) == ["Dog.md", "Veterinarian.md"]
    assert rows["Dog.md"]["content"].endswith("Links to: Cat\nUnresolved links: Vet")
    assert "Vet.md" not in state["notes"] and "Veterinarian.md" in state["notes"]


def test_deleted_note_yields_hard_delete_tombstone(tmp_path):
    root = write_vault(tmp_path, VAULT)
    state: dict = {}
    run_sync(root, state)
    (root / "pets" / "Cat.md").unlink()
    rows, deleted = run_sync(root, state)
    assert deleted == ["pets/Cat.md"]
    assert sorted(rows) == ["Dog.md", "Vet.md"]  # both linked to Cat → re-rendered
    assert rows["Vet.md"]["content"].endswith("Embeds: Dog\nUnresolved links: pets/Cat")
    assert sorted(state["notes"]) == ["Dog.md", "Vet.md"]


def test_tombstone_is_minimal_and_unchanged_notes_stay_quiet(tmp_path):
    root = write_vault(tmp_path, {"a.md": "alone", "b.md": "also alone"})
    state: dict = {}
    run_sync(root, state)
    (root / "a.md").unlink()
    emitted = list(sync_vault(root, state))
    assert emitted == [{"id": "a.md", "_deleted": True}]


def test_empty_walk_over_known_vault_skips_mass_deletion_and_keeps_state(tmp_path):
    root = write_vault(tmp_path, VAULT)
    state: dict = {}
    run_sync(root, state)
    before = json.dumps(state, sort_keys=True)
    for path in root.rglob("*.md"):
        path.unlink()
    assert run_sync(root, state) == ({}, [])
    assert json.dumps(state, sort_keys=True) == before


def test_title_change_re_renders_notes_that_link_to_it(tmp_path):
    root = write_vault(tmp_path, VAULT)
    state: dict = {}
    run_sync(root, state)
    (root / "Vet.md").write_text("---\ntitle: Animal Doctor\n---\nHello.\n", encoding="utf-8")
    bump_mtime(root / "Vet.md", 5)
    rows, _ = run_sync(root, state)
    assert sorted(rows) == ["Dog.md", "Vet.md"]
    assert rows["Dog.md"]["content"].endswith("Links to: Cat; Animal Doctor")


def test_notes_with_invalid_utf8_are_still_ingested(tmp_path):
    root = write_vault(tmp_path, {"bad.md": b"caf\xe9 [[Dog]]\n", "Dog.md": "woof"})
    rows, _ = run_sync(root, {})
    assert rows["bad.md"]["content"].startswith("caf�") and rows["Dog.md"]


def test_unreadable_note_is_skipped_not_fatal(tmp_path, monkeypatch):
    root = write_vault(tmp_path, VAULT)
    state: dict = {}
    run_sync(root, state)
    before = dict(state["notes"]["Vet.md"])
    (root / "Vet.md").write_text("Vets now treat [[Cat]] only.\n", encoding="utf-8")
    bump_mtime(root / "Vet.md", 10)
    real_read = Path.read_bytes

    def flaky(self):
        if self.name == "Vet.md":
            raise PermissionError(13, "Permission denied", str(self))
        return real_read(self)

    monkeypatch.setattr(Path, "read_bytes", flaky)
    rows, deleted = run_sync(root, state)
    # No crash, no tombstone, no cursor advance: the old entry survives untouched.
    assert rows == {} and deleted == []
    assert state["notes"]["Vet.md"] == before
    monkeypatch.setattr(Path, "read_bytes", real_read)
    rows, _ = run_sync(root, state)  # readable again → picked up on the next run
    assert list(rows) == ["Vet.md"] and rows["Vet.md"]["content"].endswith("Links to: Cat")


def test_empty_and_binary_notes_are_skipped_without_forgetting_anything(tmp_path):
    root = write_vault(tmp_path, {**VAULT, "Empty.md": "", "Binary.md": b"\x00\x01PK"})
    state: dict = {}
    rows, deleted = run_sync(root, state)
    assert sorted(rows) == ["Dog.md", "Vet.md", "pets/Cat.md"] and deleted == []
    assert "Empty.md" not in state["notes"] and "Binary.md" not in state["notes"]
    # A known note emptied by the editor keeps its last version instead of vanishing.
    (root / "Vet.md").write_text("", encoding="utf-8")
    bump_mtime(root / "Vet.md", 10)
    before = dict(state["notes"]["Vet.md"])
    assert run_sync(root, state) == ({}, [])
    assert state["notes"]["Vet.md"] == before


def test_body_only_edit_does_not_reread_linking_notes(tmp_path, monkeypatch):
    root = write_vault(tmp_path, VAULT)  # Dog and Vet link to Cat
    state: dict = {}
    run_sync(root, state)
    (root / "pets" / "Cat.md").write_text("# Cat\n\nCats nap and ignore [[Doggo]].\n")
    bump_mtime(root / "pets" / "Cat.md", 10)
    reads: list[str] = []
    real_read = Path.read_bytes
    monkeypatch.setattr(
        Path, "read_bytes", lambda self: (reads.append(self.name), real_read(self))[1]
    )
    rows, _ = run_sync(root, state)
    assert list(rows) == ["pets/Cat.md"]
    assert reads == ["Cat.md"]  # neighbours were neither read nor re-rendered


def test_title_change_re_renders_linking_notes_but_an_unused_alias_does_not(tmp_path):
    root = write_vault(tmp_path, VAULT)  # Cat links to Dog via alias "Doggo"; Vet via name
    state: dict = {}
    run_sync(root, state)
    dog = "---\ntitle: {title}\naliases: [Doggo, Pup]\n---\nDogs chase [[Cat]]s. See [[Vet]].\n"
    (root / "Dog.md").write_text(dog.format(title="Dog"))
    bump_mtime(root / "Dog.md", 10)
    rows, _ = run_sync(root, state)
    assert list(rows) == ["Dog.md"]  # adding an alias nobody uses re-renders nobody else
    (root / "Dog.md").write_text(dog.format(title="Good Dog"))
    bump_mtime(root / "Dog.md", 20)
    rows, _ = run_sync(root, state)
    assert sorted(rows) == ["Dog.md", "Vet.md", "pets/Cat.md"]  # title is in their "Links to:"
    assert rows["pets/Cat.md"]["content"].endswith("Links to: Good Dog")


def test_touched_note_is_not_re_emitted_when_a_neighbour_forces_a_re_render(tmp_path):
    root = write_vault(tmp_path, VAULT)
    state: dict = {}
    run_sync(root, state)
    bump_mtime(root / "Dog.md", 10)  # touched, not edited (iCloud, Syncthing, `touch`)
    # A new note claims the alias "Cat": every note whose links mention "cat" is re-rendered.
    write_vault(root, {"Lion.md": "---\naliases: [Cat]\n---\nBig cat.\n"})
    rows, _ = run_sync(root, state)
    # Name match still wins over alias, so Dog's rendered row is identical: only its
    # cursor moved, and the cursor is not part of the row hash → not re-emitted.
    assert list(rows) == ["Lion.md"]
    assert state["notes"]["Dog.md"]["mtime"] == (root / "Dog.md").stat().st_mtime


def test_nfd_file_name_resolves_nfc_link(tmp_path):
    nfd_name = unicodedata.normalize("NFD", "Café") + ".md"
    root = write_vault(tmp_path, {nfd_name: "Coffee.", "Menu.md": "Try the [[Café]].\n"})
    rows, _ = run_sync(root, {})
    menu_links = json.loads(rows["Menu.md"]["links"])
    assert menu_links[0]["resolved"] is not None
    assert unicodedata.normalize("NFC", menu_links[0]["resolved"]) == "Café.md"


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------


def test_obsidian_source_declares_document_marker_merge_and_hard_delete(tmp_path):
    write_vault(tmp_path, {"a.md": "x"})
    source = obsidian_source(tmp_path)
    assert getattr(source, DOCUMENT_SOURCE_ATTR) == OBSIDIAN_SOURCE_NAME
    assert source.name == OBSIDIAN_SOURCE_NAME
    # Own pipeline scope per vault: cognee must not fall back to the shared pipeline name.
    assert getattr(source, PIPELINE_SCOPE_ATTR) == f"obsidian:{tmp_path.resolve().as_posix()}"
    assert pipeline_name_for_source(source, "ds") != "ingest_dlt_source"
    assert pipeline_name_for_source(source, "ds") != pipeline_name_for_source(source, "other")
    table = source.resources[OBSIDIAN_TABLE_NAME].compute_table_schema()
    assert table["write_disposition"] == "merge"
    assert table["columns"]["id"]["primary_key"] is True
    assert table["columns"]["_deleted"]["hard_delete"] is True
    assert table["columns"]["_deleted"]["data_type"] == "bool"


def test_obsidian_source_reads_vault_from_environment(tmp_path, monkeypatch):
    write_vault(tmp_path, {"a.md": "x"})
    monkeypatch.setenv("OBSIDIAN_VAULT_PATH", str(tmp_path))
    assert obsidian_source().name == OBSIDIAN_SOURCE_NAME
    monkeypatch.delenv("OBSIDIAN_VAULT_PATH")
    with pytest.raises(ValueError, match="OBSIDIAN_VAULT_PATH"):
        obsidian_source()


def test_obsidian_source_rejects_missing_directory(tmp_path):
    with pytest.raises(ValueError, match="not a directory"):
        obsidian_source(tmp_path / "nope")


# ---------------------------------------------------------------------------
# End-to-end: a real dlt merge acts on the hard-delete marker
# ---------------------------------------------------------------------------


def _pipeline(dlt, tmp_path):
    return dlt.pipeline(
        pipeline_name="obsidian_test",
        destination=dlt.destinations.sqlalchemy(
            f"sqlite:///{(tmp_path / 'obsidian.db').as_posix()}"
        ),
        dataset_name="obsidian_ds",
        pipelines_dir=str(tmp_path / "state"),
    )


def _table(pipeline) -> dict[str, dict]:
    with (
        pipeline.sql_client() as client,
        client.execute_query(
            f"SELECT id, title, content, link_count FROM {OBSIDIAN_TABLE_NAME} ORDER BY id"
        ) as cursor,
    ):
        return {
            r[0]: {"title": r[1], "content": r[2], "link_count": r[3]} for r in cursor.fetchall()
        }


def test_edit_add_delete_end_to_end_through_a_real_dlt_merge(tmp_path):
    dlt = pytest.importorskip("dlt")
    root = write_vault(tmp_path / "vault", VAULT)
    pipeline = _pipeline(dlt, tmp_path)

    # Sync #1: three notes land in the destination.
    pipeline.run(obsidian_source(root), write_disposition="merge", primary_key="id")
    first = _table(pipeline)
    assert sorted(first) == ["Dog.md", "Vet.md", "pets/Cat.md"]
    assert first["Dog.md"]["link_count"] == 2

    # Sync #2: one edit, one add, one delete. State came back from dlt, not from us.
    (root / "Vet.md").write_text("Vets treat [[Dog]] and the new [[Bird]].\n", encoding="utf-8")
    bump_mtime(root / "Vet.md", 10)
    write_vault(root, {"Bird.md": "---\ntitle: Bird\n---\nTweet [[Cat]]."})
    (root / "pets" / "Cat.md").unlink()
    pipeline.run(obsidian_source(root), write_disposition="merge", primary_key="id")
    second = _table(pipeline)
    assert sorted(second) == ["Bird.md", "Dog.md", "Vet.md"]  # Cat forgotten, Bird added
    assert second["Vet.md"]["content"].endswith("Links to: Dog; Bird")
    assert second["Dog.md"]["content"].endswith("Links to: Vet\nUnresolved links: Cat")
    assert second["Bird.md"]["content"].endswith("Unresolved links: Cat")

    # Sync #3: nothing changed → no load package, table identical.
    info = pipeline.run(obsidian_source(root), write_disposition="merge", primary_key="id")
    assert not info.load_packages
    assert _table(pipeline) == second
