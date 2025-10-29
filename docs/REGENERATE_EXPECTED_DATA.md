# Regenerating Expected Test Data

## Purpose

This document explains how to regenerate expected test data files when core functionality changes (e.g., embedding generation algorithm modifications).

## Background

The test suite (`tests/test_dataset.py`) validates the dataset creation pipeline by comparing generated outputs against pre-computed expected files. When core algorithms change (like embedding generation), the expected files must be regenerated to reflect the new correct outputs.

## Problem Statement

**Situation**: Embedding generation code was modified, causing tests to fail because generated embeddings no longer match the old expected embeddings.

**Solution**: Regenerate all expected test data files with the new embedding algorithm to establish new ground truth.

## Test Data Structure

### Directory Layout

```
tests/data/
├── dataset_expected/                    # Current expected outputs (ground truth)
│   ├── embeddings/                     # ESM2 embeddings (79 MB) - 5 datasets
│   ├── distograms/                     # Distance matrices (14 MB)
│   ├── structures/                     # PDB structures (2.5 MB)
│   ├── coordinates/                    # Coordinate files (352 KB)
│   ├── sequences/                      # FASTA files (24 KB)
│   ├── datasets/                       # Index/metadata files (244 KB)
│   └── input_lists/                    # Input PDB ID lists
│       ├── pdb_5.txt                   # 5 PDB IDs: 1aa6, 1fdi, 1fdo, 1pae, 2iv2
│       └── pdb_7.txt                   # 7 PDB IDs: above 5 + 3eao, 1brs
│
├── dataset_generated/                   # Test outputs (created during test runs)
│   └── [same structure as dataset_expected]
│
├── dataset_expected_backup_*/          # Timestamped backups
└── dataset_expected_backup_2/          # New generated outputs for review
```

### File Breakdown (per dataset)

Each of the 5 test datasets (`initial_5`, `second_5`, `third_7`, `fourth_7`, `fifth_7`) contains:

1. **Embeddings** (~15 MB each): `embeddings/PDB-subset--{name}/batch_0.h5`
   - ESM2_T33_650M model outputs
   - Largest files in the test data

2. **Distograms**: `distograms/PDB-subset--{name}/batch_*.h5`
   - CA (alpha carbon) distance matrices

3. **Structures**: `structures/PDB/subset_/{name}/0/pdbs.h5`
   - Downloaded/processed PDB files

4. **Coordinates**: `coordinates/PDB-subset--{name}/batch_0_ca.h5`
   - Extracted 3D coordinates

5. **Sequences**: `sequences/PDB-subset--{name}_ca.fasta`
   - FASTA format sequences

6. **Metadata/Indexes** (~11 files per dataset):
   - `datasets/PDB-subset--{name}/dataset.json`
   - `datasets/PDB-subset--{name}/*.idx` (10 index files)

**Total**: ~96 MB across 76 files

## Test Scenarios

The test suite runs 5 incremental scenarios to test dataset reuse logic:

| Test | Dataset | Input | Overwrite | Expected Behavior |
|------|---------|-------|-----------|-------------------|
| 1 | `initial_5` | pdb_5.txt | No | Create 5 new PDB structures |
| 2 | `second_5` | pdb_5.txt | Yes | Create 5 new (force overwrite) |
| 3 | `third_7` | pdb_7.txt | No | Reuse 5 from second_5, add 2 new |
| 4 | `fourth_7` | pdb_7.txt | Yes | Create 7 new (force overwrite) |
| 5 | `fifth_7` | pdb_7.txt | No | Reuse all 7 from fourth_7 |

## Regeneration Script

### Location
`tests/regenerate_expected_data.py`

### What It Does

1. **Backs up current expected data** to `dataset_expected_backup_MM_dd_hh_mm_ss/`
2. **Generates new outputs** to `dataset_expected_backup_2/`
3. **Runs all 5 test scenarios** without assertions or comparisons
4. **Creates all output types**: embeddings, distograms, sequences, structures, coordinates, indexes
5. **Uses Dask cluster** for parallel processing (same as tests)
6. **Times each operation** for performance tracking

### Key Features

- ✅ No file cleanup (all outputs preserved)
- ✅ No assertions (pure generation)
- ✅ Timestamped backup of current expected data
- ✅ Same configuration as actual tests
- ✅ Clear progress logging with emojis
- ✅ Error handling with traceback

## Usage Instructions

### Prerequisites

1. Activate the conda environment:
   ```bash
   eval "$(mamba shell hook --shell bash)"
   mamba activate /mnt/vdb2/var/storage/deepfri2/toolbox/tbe
   ```

2. Navigate to project root:
   ```bash
   cd /mnt/vdb2/var/storage/deepfri2/toolbox/deepFRI2-toolbox-dev
   ```

### Run the Regeneration Script

```bash
PYTHONPATH=/mnt/vdb2/var/storage/deepfri2/toolbox/deepFRI2-toolbox-dev:$PYTHONPATH python tests/regenerate_expected_data.py
```

### Expected Output

```
======================================================================
🔄 EXPECTED TEST DATA REGENERATION SCRIPT
======================================================================

[STEP 1] Backup current expected data
📦 Backing up current expected data...
   From: tests/data/dataset_expected
   To:   tests/data/dataset_expected_backup_10_22_14_30_45
✅ Backup completed: tests/data/dataset_expected_backup_10_22_14_30_45

[STEP 2] Prepare output directory
✅ Output directory ready: tests/data/dataset_expected_backup_2

[STEP 3] Setup Dask cluster
🔧 Setting up Dask cluster with 15 workers...
✅ Dask cluster ready: <Client: 'tcp://127.0.0.1:...' ...>

[STEP 4] Run all test scenarios

######################################################################
# TEST 1/5: initial_5
# Create initial dataset with 5 PDB structures
######################################################################
... [processing details] ...

✅ Dataset 'initial_5' completed in 45.32s

... [4 more tests] ...

======================================================================
🎉 ALL TESTS COMPLETED SUCCESSFULLY
======================================================================
Total execution time: 234.56s (3.91m)

Generated outputs location:
  tests/data/dataset_expected_backup_2

Backup location:
  tests/data/dataset_expected_backup_10_22_14_30_45

📋 Next steps:
  1. Manually verify the generated outputs
  2. Compare with current expected files if needed
  3. Replace tests/data/dataset_expected with dataset_expected_backup_2
```

### Execution Time

Expected runtime: **~4-6 minutes** (depends on CPU cores and embedding generation speed)

## Verification Steps

After regeneration, manually verify the new outputs:

### 1. Check File Structure

```bash
# Compare directory structure
diff <(cd tests/data/dataset_expected && find . -type f | sort) \
     <(cd tests/data/dataset_expected_backup_2 && find . -type f | sort)
```

### 2. Check File Sizes

```bash
# Compare file sizes
du -sh tests/data/dataset_expected/*
du -sh tests/data/dataset_expected_backup_2/*
```

### 3. Inspect Embeddings (most critical)

```bash
# Use h5dump or Python to inspect embedding files
python -c "
import h5py
with h5py.File('tests/data/dataset_expected_backup_2/embeddings/PDB-subset--initial_5/batch_0.h5', 'r') as f:
    print('Keys:', list(f.keys()))
    for key in f.keys():
        print(f'{key}: shape={f[key].shape}, dtype={f[key].dtype}')
"
```

### 4. Check Index Files

```bash
# Verify index file structure
cat tests/data/dataset_expected_backup_2/datasets/PDB-subset--initial_5/dataset.json | jq .
```

### 5. Run Diff on Small Files

```bash
# Compare FASTA files (text-based)
diff tests/data/dataset_expected/sequences/PDB-subset--initial_5_ca.fasta \
     tests/data/dataset_expected_backup_2/sequences/PDB-subset--initial_5_ca.fasta
```

## Replacing Expected Files

Once verified, replace the current expected files:

### Option 1: Direct Replacement (CAREFUL!)

```bash
# Remove old expected data
rm -rf tests/data/dataset_expected

# Move new data to expected location
mv tests/data/dataset_expected_backup_2 tests/data/dataset_expected
```

### Option 2: Safe Replacement with Additional Backup

```bash
# Create another backup just to be safe
cp -r tests/data/dataset_expected tests/data/dataset_expected_backup_before_replacement

# Remove old expected data
rm -rf tests/data/dataset_expected

# Move new data to expected location
mv tests/data/dataset_expected_backup_2 tests/data/dataset_expected
```

### Option 3: Swap (Safer)

```bash
# Rename current to backup
mv tests/data/dataset_expected tests/data/dataset_expected_old

# Rename new to current
mv tests/data/dataset_expected_backup_2 tests/data/dataset_expected

# If something goes wrong, you can easily swap back:
# mv tests/data/dataset_expected tests/data/dataset_expected_backup_2
# mv tests/data/dataset_expected_old tests/data/dataset_expected
```

## Running Tests After Replacement

Verify the tests pass with the new expected files:

```bash
# Run all dataset tests
python -m pytest tests/test_dataset.py -v

# Run specific test
python -m pytest tests/test_dataset.py::test1_initial_5 -v
```

## Troubleshooting

### Script Fails During Execution

- **Check Dask cluster**: Ensure port 8990 is not in use
- **Check disk space**: Regeneration needs ~100 MB free space
- **Check conda environment**: Ensure `tbe` environment is activated
- **Check input files**: Verify `pdb_5.txt` and `pdb_7.txt` exist in `dataset_expected/input_lists/`

### Tests Still Fail After Replacement

- **Numerical precision**: Embedding comparisons use tolerances (rtol=1e-4, atol=1e-5)
- **Random seed**: Ensure embedding generation is deterministic
- **Check modifications**: Verify all code changes are compatible with test logic

### Outputs Look Different

- **Expected for embedding changes**: If you modified embedding generation, outputs WILL be different
- **Verify correctness**: Check that changes are intentional and scientifically valid
- **Check logs**: Review generation logs for warnings or errors

## File Locations Reference

| Path | Description |
|------|-------------|
| `tests/regenerate_expected_data.py` | Regeneration script |
| `tests/test_dataset.py` | Original test file |
| `tests/data/dataset_expected/` | Current expected outputs (ground truth) |
| `tests/data/dataset_expected_backup_2/` | New generated outputs for review |
| `tests/data/dataset_expected_backup_*/` | Timestamped backups |
| `tests/data/dataset_generated/` | Test run outputs (cleaned between runs) |

## Notes

- The script does NOT modify `dataset_expected` - you must manually replace it after verification
- Timestamped backups allow you to track when expected data was regenerated
- The script uses the same config as tests: CA distograms, inf threshold, ESM2_T33_650M embedder
- All 5 test scenarios must run successfully to ensure incremental reuse logic works correctly

## Related Files

- `tests/test_dataset.py` - Main dataset test suite
- `tests/utils.py` - Comparison utilities (compare_h5_files, compare_fasta_files, etc.)
- `tests/paths.py` - Test path definitions (OUTPATH, EXPPATH)
- `toolbox/models/manage_dataset/structures_dataset.py` - Dataset creation logic
- `toolbox/models/embedding/embedder/esm2_embedder.py` - ESM2 embedding generation

## History

- **2025-10-22**: Created regeneration script to handle embedding generation changes
- Script created to avoid manual recreation of 96 MB / 76 files across 5 test datasets
