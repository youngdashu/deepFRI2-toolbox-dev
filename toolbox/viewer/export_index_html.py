from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple, Any
import json
import re

from toolbox.config import Config, load_config


# ----------------------------
# Data models
# ----------------------------

@dataclass
class DatasetIdentity:
    db: str
    collection: str
    slug: str

    def folder_name(self) -> str:
        return f"{self.db}-{self.collection}--{self.slug}"


@dataclass
class DatasetMeta:
    identity: DatasetIdentity
    path: Path  # dataset directory containing dataset.json


@dataclass
class IndexPaths:
    forward: Optional[Path]
    reversed: Optional[Path]


# ----------------------------
# Config and roots
# ----------------------------

def resolve_datasets_root(config: Config, override_root: Optional[Path] = None) -> Path:
    base = Path(config.data_path)
    root = override_root or (base / "datasets")
    return Path(root).resolve()


# ----------------------------
# Discovery helpers
# ----------------------------

_FOLDER_NAME_RE = re.compile(
    r"^(?P<db>PDB|AFDB|ESMatlas|other)-(?P<coll>all|clust|part|subset)--(?P<slug>.+)$"
)


def _parse_identity_from_folder_name(name: str) -> Optional[DatasetIdentity]:
    m = _FOLDER_NAME_RE.match(name)
    if not m:
        return None
    return DatasetIdentity(db=m.group("db"), collection=m.group("coll"), slug=m.group("slug"))


def discover_dataset(
    dataset_arg: Optional[Path],
    datasets_root: Path,
    dataset_slug: Optional[str],
) -> DatasetMeta:
    # Case 1: dataset path provided (dir or dataset.json)
    if dataset_arg:
        dataset_arg = Path(dataset_arg).resolve()
        if dataset_arg.is_file() and dataset_arg.name == "dataset.json":
            dataset_dir = dataset_arg.parent
        elif dataset_arg.is_dir():
            dataset_dir = dataset_arg
        else:
            raise FileNotFoundError(f"Invalid dataset reference: {dataset_arg}")

        identity = _parse_identity_from_folder_name(dataset_dir.name)
        if identity is None:
            raise ValueError(
                f"Dataset folder name does not match pattern <DB>-<COLL>--<slug>: {dataset_dir.name}"
            )
        return DatasetMeta(identity=identity, path=dataset_dir)

    # Case 2: discover by slug under datasets root
    if dataset_slug:
        matches = [d for d in (datasets_root.glob("*/")) if d.is_dir() and d.name.endswith(f"--{dataset_slug}")]
        # Prefer exact enum prefix match if multiple
        for d in matches:
            identity = _parse_identity_from_folder_name(d.name)
            if identity:
                return DatasetMeta(identity=identity, path=d)
        raise FileNotFoundError(f"Dataset with slug '{dataset_slug}' not found under {datasets_root}")

    # Case 3: nothing specified -> attempt to pick latest or raise
    raise ValueError("Either --dataset or --dataset-slug must be provided")


def find_index_files(dataset_dir: Path) -> Dict[str, IndexPaths]:
    types = ["dataset", "sequences", "coordinates", "embeddings", "distograms"]
    results: Dict[str, IndexPaths] = {}
    for t in types:
        fwd = dataset_dir / f"{t}.idx"
        rev = dataset_dir / f"{t}_reversed.idx"
        results[t] = IndexPaths(forward=fwd if fwd.exists() else None, reversed=rev if rev.exists() else None)
    return results


# ----------------------------
# Parsing helpers
# ----------------------------

def stream_parse_idx(path: Path) -> Iterator[Tuple[str, Any]]:
    """Yield (key, value) pairs from a top-level JSON object index file.

    This implementation loads the JSON once and streams items; suitable for moderate file sizes.
    """
    with path.open("r") as f:
        data = json.load(f)
    if isinstance(data, dict):
        for k, v in data.items():
            yield k, v
    else:
        # Accept list of pairs [[k, v], ...] as fallback
        for item in data:
            if isinstance(item, list) and len(item) == 2:
                yield item[0], item[1]


def extract_dataset_identity_from_path(path: str) -> Optional[DatasetIdentity]:
    # Pattern A: hyphenated segment
    m = re.search(
        r"(?P<db>PDB|AFDB|ESMatlas|other)-(?P<coll>all|clust|part|subset)--(?P<slug>[^/]+)",
        path,
    )
    if m:
        return DatasetIdentity(m.group("db"), m.group("coll"), m.group("slug"))

    # Pattern B: structured structures/<DB>/<COLL>_<slug>
    m2 = re.search(
        r"(?:^|/)structures/(?P<db>PDB|AFDB|ESMatlas|other)/(?P<coll>all|clust|part|subset)_(?:/)?(?P<slug>[^/]+)/",
        path,
    )
    if m2:
        return DatasetIdentity(m2.group("db"), m2.group("coll"), m2.group("slug"))

    return None


def extract_batch_id_from_path(path: str, index_type: str) -> Optional[str]:
    if index_type in ("distograms", "embeddings"):
        m = re.search(r"(?:^|/)(distograms|embeddings)/[^/]+/batch_(?P<batch>\d+)\.h5$", path)
        return m.group("batch") if m else None
    if index_type == "coordinates":
        m = re.search(r"(?:^|/)coordinates/[^/]+/batch_(?P<batch>\d+)_ca\.h5$", path)
        return m.group("batch") if m else None
    if index_type in ("dataset", "structures"):
        m = re.search(
            r"(?:^|/)structures/(PDB|AFDB|ESMatlas|other)/(all|clust|part|subset)_(?:/)?[^/]+/(?P<batch>\d+)/pdbs\.h5$",
            path,
        )
        return m.group("batch") if m else None
    if index_type == "sequences":
        return None
    return None


# ----------------------------
# Statistics
# ----------------------------

def compute_index_stats(
    index_type: str,
    forward_path: Optional[Path],
    reversed_path: Optional[Path],
) -> Dict[str, Any]:
    forward_present = bool(forward_path and forward_path.exists())
    reversed_present = bool(reversed_path and reversed_path.exists())

    num_proteins_forward = 0
    num_files_referenced = 0
    num_edges_reversed = 0

    by_dataset: Dict[str, Dict[str, Any]] = {}

    if forward_present:
        # forward maps protein_id -> file path
        try:
            for _k, _v in stream_parse_idx(forward_path):
                num_proteins_forward += 1
        except Exception:
            # Be robust if malformed
            num_proteins_forward = 0

    if reversed_present:
        seen_files = set()
        for file_path_str, protein_ids in stream_parse_idx(reversed_path):
            seen_files.add(file_path_str)
            # protein_ids can be list or int
            if isinstance(protein_ids, list):
                count_refs = len(protein_ids)
            else:
                # If value is a single id or count
                try:
                    count_refs = int(protein_ids)
                except Exception:
                    count_refs = 1
            num_edges_reversed += count_refs

            # extract identity and batch for grouping
            identity = extract_dataset_identity_from_path(file_path_str)
            slug = identity.slug if identity else "unknown"
            ds_entry = by_dataset.setdefault(
                slug, {"files_referenced": 0, "proteins_referencing": 0, "is_self": None, "files_per_batch": {}}
            )
            ds_entry["files_referenced"] += 1
            ds_entry["proteins_referencing"] += count_refs

            batch_id = extract_batch_id_from_path(file_path_str, index_type) or "-"
            ds_entry["files_per_batch"][batch_id] = ds_entry["files_per_batch"].get(batch_id, 0) + 1

        num_files_referenced = len(seen_files)

    result = {
        "index_type": index_type,
        "forward_present": forward_present,
        "reversed_present": reversed_present,
        "num_proteins_forward": num_proteins_forward,
        "num_files_referenced": num_files_referenced,
        "num_edges_reversed": num_edges_reversed,
        "by_dataset": by_dataset,
    }
    return result


def compute_global_rollup(per_index_stats: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    aggregate: Dict[str, Dict[str, int]] = {}
    for _t, stats in per_index_stats.items():
        for slug, entry in stats.get("by_dataset", {}).items():
            agg = aggregate.setdefault(slug, {"files_referenced": 0, "proteins_referencing": 0})
            agg["files_referenced"] += entry.get("files_referenced", 0)
            agg["proteins_referencing"] += entry.get("proteins_referencing", 0)

    # Top list by files_referenced then proteins_referencing
    top = sorted(
        (
            {
                "slug": slug,
                "files_referenced": v["files_referenced"],
                "proteins_referencing": v["proteins_referencing"],
            }
            for slug, v in aggregate.items()
        ),
        key=lambda x: (x["files_referenced"], x["proteins_referencing"]),
        reverse=True,
    )

    return {"aggregate": aggregate, "top": top[:50]}


# ----------------------------
# HTML rendering
# ----------------------------

def _build_html(summary_payload: Dict[str, Any], report_name: str) -> str:
    data_json = json.dumps(summary_payload)
    # Simple HTML with embedded JSON and very light UI
    return f"""
<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{report_name}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; margin-right: 6px; }}
    .ok {{ background: #e8f5e9; color: #1b5e20; }}
    .no {{ background: #ffebee; color: #b71c1c; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: left; }}
    th {{ background: #fafafa; }}
    .panel {{ border: 1px solid #eee; padding: 12px; border-radius: 6px; margin: 12px 0; }}
    details > summary {{ cursor: pointer; }}
    .muted {{ color: #666; }}
    .controls {{ margin: 8px 0; }}
  </style>
  <script type=\"application/json\" id=\"summary-data\">{data_json}</script>
  <script>
    function $(id) {{ return document.getElementById(id); }}
    function render() {{
      const payload = JSON.parse($("summary-data").textContent);
      const root = $("root");
      root.innerHTML = '';
      const header = document.createElement('div');
      header.innerHTML = `<h2>${{payload.dataset.identity.folder_name}}</h2><div class=\"muted\">${{payload.dataset.path}}</div>`;
      root.appendChild(header);

      // Index discovery summary
      const disc = document.createElement('div');
      disc.className = 'panel';
      disc.innerHTML = '<h3>Index discovery</h3>';
      const ul = document.createElement('ul');
      for (const [t, p] of Object.entries(payload.index_paths)) {{
        const li = document.createElement('li');
        const fwd = p.forward ? 'present' : 'missing';
        const rev = p.reversed ? 'present' : 'missing';
        li.textContent = `${{t}}: forward=${{fwd}}, reversed=${{rev}}`;
        ul.appendChild(li);
      }}
      disc.appendChild(ul);
      root.appendChild(disc);

      // Per-index panels
      for (const [t, stats] of Object.entries(payload.per_index)) {{
        const panel = document.createElement('div');
        panel.className = 'panel';
        const badges = `
          <span class=\"badge ${{stats.forward_present ? 'ok' : 'no'}}\">forward</span>
          <span class=\"badge ${{stats.reversed_present ? 'ok' : 'no'}}\">reversed</span>`;
        panel.innerHTML = `<h3>${{t}}</h3>${{badges}}
          <div class=\"muted\">proteins_forward=${{stats.num_proteins_forward}}, files_referenced=${{stats.num_files_referenced}}, edges_reversed=${{stats.num_edges_reversed}}`;

        const ds = stats.by_dataset || {{}};
        const keys = Object.keys(ds);
        if (keys.length) {{
          const table = document.createElement('table');
          table.innerHTML = '<thead><tr><th>dataset slug</th><th>files referenced</th><th>proteins referencing</th></tr></thead>';
          const tbody = document.createElement('tbody');
          for (const slug of keys.sort()) {{
            const row = document.createElement('tr');
            row.innerHTML = `<td>${{slug}}</td><td>${{ds[slug].files_referenced||0}}</td><td>${{ds[slug].proteins_referencing||0}}</td>`;
            tbody.appendChild(row);
          }}
          table.appendChild(tbody);
          panel.appendChild(table);

          const details = document.createElement('details');
          details.innerHTML = '<summary>Show batches per dataset</summary>';
          for (const slug of keys.sort()) {{
            const sub = document.createElement('div');
            const batches = ds[slug].files_per_batch || {{}};
            const subt = document.createElement('table');
            subt.innerHTML = '<thead><tr><th>dataset slug</th><th>batch id</th><th>files</th></tr></thead>';
            const sb = document.createElement('tbody');
            for (const [b, n] of Object.entries(batches)) {{
              const r = document.createElement('tr');
              r.innerHTML = `<td>${{slug}}</td><td>${{b}}</td><td>${{n}}</td>`;
              sb.appendChild(r);
            }}
            subt.appendChild(sb);
            sub.appendChild(subt);
            details.appendChild(sub);
          }}
          panel.appendChild(details);
        }} else {{
          const em = document.createElement('div');
          em.className = 'muted';
          em.textContent = 'No reversed references';
          panel.appendChild(em);
        }}
        root.appendChild(panel);
      }}

      // Global rollup
      const roll = document.createElement('div');
      roll.className = 'panel';
      roll.innerHTML = '<h3>Global rollup</h3>';
      const table = document.createElement('table');
      table.innerHTML = '<thead><tr><th>dataset slug</th><th>files referenced</th><th>proteins referencing</th></tr></thead>';
      const tb = document.createElement('tbody');
      for (const row of payload.global.top) {{
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${{row.slug}}</td><td>${{row.files_referenced}}</td><td>${{row.proteins_referencing}}</td>`;
        tb.appendChild(tr);
      }}
      table.appendChild(tb);
      roll.appendChild(table);
      root.appendChild(roll);
    }}
    window.addEventListener('DOMContentLoaded', render);
  </script>
</head>
<body>
  <div id=\"root\"></div>
</body>
</html>
"""


def render_html(summary_payload: Dict[str, Any], output_path: Path, report_name: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    html = _build_html(summary_payload, report_name)
    output_path.write_text(html)


# ----------------------------
# Orchestration
# ----------------------------

def export_index_view(
    config: Config,
    dataset: Optional[Path] = None,
    dataset_slug: Optional[str] = None,
    root: Optional[Path] = None,
    index_types: Optional[List[str]] = None,
    output_dir: Optional[Path] = None,
) -> Path:
    datasets_root = resolve_datasets_root(config, root)
    meta = discover_dataset(dataset, datasets_root, dataset_slug)

    index_paths = find_index_files(meta.path)
    selected_types = index_types or list(index_paths.keys())
    per_index: Dict[str, Dict[str, Any]] = {}
    for t in selected_types:
        paths = index_paths.get(t)
        if not paths:
            continue
        stats = compute_index_stats(t, paths.forward, paths.reversed)
        # Mark is_self
        for slug, entry in stats.get("by_dataset", {}).items():
            entry["is_self"] = slug == meta.identity.slug
        per_index[t] = stats

    global_rollup = compute_global_rollup(per_index)

    payload = {
        "dataset": {
            "identity": {
                "db": meta.identity.db,
                "collection": meta.identity.collection,
                "slug": meta.identity.slug,
                "folder_name": meta.identity.folder_name(),
            },
            "path": str(meta.path),
        },
        "index_paths": {k: {"forward": (str(v.forward) if v.forward else None), "reversed": (str(v.reversed) if v.reversed else None)} for k, v in index_paths.items()},
        "per_index": per_index,
        "global": global_rollup,
    }

    reports_dir = (output_dir or (Path(__file__).resolve().parents[2] / "reports")).resolve()
    out_file = reports_dir / f"{meta.path.name}.html"
    render_html(payload, out_file, meta.identity.folder_name())
    return out_file


__all__ = [
    "resolve_datasets_root",
    "discover_dataset",
    "find_index_files",
    "stream_parse_idx",
    "extract_dataset_identity_from_path",
    "extract_batch_id_from_path",
    "compute_index_stats",
    "compute_global_rollup",
    "render_html",
    "export_index_view",
]


