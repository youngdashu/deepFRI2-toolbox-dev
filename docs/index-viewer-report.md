## “Index Indexes Viewer” — HTML Report (no server, no raw idx embedding)

- **Goal**: Generate a single, self-contained HTML report in `reports/` that summarizes a dataset’s indexes:
  - Crucial statistics per index type.
  - Cross-dataset reuse by dataset and by batch (counts).
- **Data flow**: Compute summaries at generation time; embed only compact summary JSON inside the HTML (not raw `.idx`). No HTTP server is needed to view the report.

### Inputs, discovery, and identity
- **Datasets root**: Read `data_path` from project config (`toolbox/config.py` → `config.json`); default datasets root is `<data_path>/datasets`.
- **Dataset selection**:
  - CLI accepts `--dataset` (path to dataset dir or its `dataset.json`) or `--dataset-slug`.
  - If neither provided, scan `<data_path>/datasets` and present a picker (non-interactive fallback via `--dataset-slug`).
- **Dataset identity**:
  - Parse `{db, collection, slug}` from directory name/path segments.
  - Database and collection enums:
    - `db ∈ {PDB, AFDB, ESMatlas, other}`
    - `collection ∈ {all, clust, part, subset}`
  - Folder name pattern (base name): `<DB>-<COLL>--{slug}` (e.g., `PDB-subset--fifth_7`).
  - Use the dataset folder name as the report filename: `reports/<folder-name>.html`.

### In-scope index types (read forward and reversed)
- Types: `dataset`, `sequences`, `coordinates`, `embeddings`, `distograms`.
- Reversed naming: `<type>_reversed.idx` (primary for origin/batch statistics).
- Also read forward `.idx` for presence/coverage reconciliation (no separate "missing" report in v1).

### Path schemas and batch extraction (generalized)
- **Referenced dataset slug detection**:
  - Pattern A (hyphenated): segment `<DB>-<COLL>--{slug}` appears in path.
  - Pattern B (structured): segments `.../structures/<DB>/<COLL>_/{slug}/...`.
- **Batch extraction patterns** (implement; return `None` when not applicable):
```regex
# Base name composed of enum tokens
(?P<db>PDB|AFDB|ESMatlas|other)-(?P<coll>all|clust|part|subset)--(?P<slug>[^/]+)

# distograms & embeddings: file batches
/(distograms|embeddings)/(?P<base>(?P<db>PDB|AFDB|ESMatlas|other)-(?P<coll>all|clust|part|subset)--(?P<slug>[^/]+))/batch_(?P<batch>\d+)\.h5$

# coordinates batches (with CA suffix)
/coordinates/(?P<base>(?P<db>PDB|AFDB|ESMatlas|other)-(?P<coll>all|clust|part|subset)--(?P<slug>[^/]+))/batch_(?P<batch>\d+)_ca\.h5$

# sequences (no batch; CA fasta)
/sequences/(?P<base>(?P<db>PDB|AFDB|ESMatlas|other)-(?P<coll>all|clust|part|subset)--(?P<slug>[^/]+))_ca\.fasta$

# dataset/structures batches: directory level number
/structures/(?P<db>PDB|AFDB|ESMatlas|other)/(?P<coll>all|clust|part|subset)_(?P<slug>[^/]+)/(?P<batch>\d+)/pdbs\.h5$
```
- Notes from `PDB-subset--fifth_7` samples:
  - References point to `...PDB-subset--fourth_7...`.
  - Batches appear as `batch_0.h5`, `batch_0_ca.h5`, or directory `.../{slug}/0/pdbs.h5`.
  - Sequences have no batch id (`..._ca.fasta`).

### Statistics to compute and embed
Per index type:
- **Presence**: `forward_present`, `reversed_present`.
- **Sizes**:
  - `num_proteins_forward` (unique keys in forward)
  - `num_files_referenced` (unique file targets from reversed)
  - `num_edges_reversed` (sum of protein references across reversed entries)
- **Cross-dataset reuse** (from reversed):
  - By dataset (`to_dataset_slug`): `files_referenced`, `proteins_referencing`, `is_self`.
  - By dataset+batch: `files_per_batch` counts per `(slug, batch)`.
- **Examples**: up to N example file paths per referenced dataset (small cap).
- **Reconciliation**: high-level `num_proteins_reversed` vs `num_proteins_forward`.

Global (across all index types):
- Aggregate referenced datasets with totals and a top list (by `files_referenced`, `proteins_referencing`).

Embed the compact summary as a single `<script type="application/json" id="summary-data">…</script>` within the HTML.

### Output
- **Location**: write only to `<repo_root>/reports/`.
- **Filename**: exactly the dataset folder name plus `.html` (e.g., `PDB-subset--fifth_7.html`).

### HTML structure (no fetch, no server)
- Minimal vanilla HTML/JS/CSS; optional CDN for icons/utility CSS; no graphs in v1.
- **Sections**:
  - Header: dataset identity and dataset path (resolved via config’s `data_path`).
  - Index discovery summary (present `.idx` files).
  - Per-index panels:
    - Presence badges and size metrics.
    - Dataset reuse table (slug, files referenced, proteins referencing, self/external).
    - Collapsible batch table per dataset (batch id → file count).
    - Optional tiny bar charts (inline SVG) for top datasets.
  - Global rollup: top referenced datasets across all types.
  - Controls: text filter (slug substring), CSV export buttons per table.
- **Scale UX**: paginate/virtualize long tables; cap examples.

### CLI
- **Command**: `export-index-view`
- **Args**:
  - `--dataset` (path) or `--dataset-slug` (string)
  - `--root` (override datasets root; default `<data_path>/datasets`)
  - `--index-types` (comma-separated; default `all`)
  - `--output-dir` (default `<repo_root>/reports`)
- **Behavior**:
  - Resolve `data_path` from config; compute default root.
  - Discover `.idx` per index type; read reversed first, optionally forward.
  - Compute stats; render a single HTML with embedded summary data.
  - No symlinks; no writes in the dataset directory.

### Implementation outline (Python)
- Module: `toolbox/viewer/export_index_html.py`
  - `load_config() -> Config`
  - `resolve_datasets_root(config) -> Path`
  - `discover_dataset(dataset_arg, root, dataset_slug) -> DatasetMeta`
  - `find_index_files(dataset_dir) -> Dict[str, IndexPaths]`
  - `stream_parse_idx(path: Path) -> Iterator[Tuple[key, value]]` (stream JSON)
  - `extract_dataset_identity_from_path(path: str) -> DatasetIdentity` (supports both schemas)
  - `extract_batch_id_from_path(path: str, index_type: str) -> Optional[str>` (regexes above)
  - `compute_index_stats(index_type, forward_path, reversed_path) -> Dict`
  - `compute_global_rollup(per_index_stats: Dict) -> Dict`
  - `render_html(summary_payload: Dict, output_path: Path, report_name: str)`
- CLI integration: add `export-index-view` subcommand in `toolbox/scripts/command_parser.py` (wire to above).

### Testing targets (using `tests/data/dataset_expected/datasets/*`)
- Config loading and datasets root resolution from `config.json`.
- Dataset discovery by path and slug.
- Index discovery for the five types; graceful handling when missing.
- Batch extraction correctness for:
  - distograms/embeddings: `batch_<n>.h5`
  - coordinates: `batch_<n>_ca.h5`
  - sequences: no batch
  - dataset/structures: `.../structures/<DB>/<COLL>_/<slug>/<n>/pdbs.h5`
- Cross-dataset detection (e.g., `...<DB>-<COLL>--fourth_7...` referenced from `...<DB>-<COLL>--fifth_7...`).
- Computed stats and global rollup keys/values present in embedded JSON.
- HTML exists in `reports/` with filename equal to dataset folder name.

### Acceptance criteria
- Running `export-index-view` produces `reports/<dataset-folder-name>.html`.
- The report opens directly from disk (no server).
- It shows:
  - Presence/size stats per index type.
  - Cross-dataset reuse by dataset and by batch, aggregated from reversed indexes.
  - Global rollup of referenced datasets.
- Reads both forward and reversed indexes; uses reversed as primary for origin stats.
- Supports the identity/path schemas and batch patterns listed above.
