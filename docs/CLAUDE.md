# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

### Activate Python Environment

```bash
eval "$(mamba shell hook --shell bash)"
mamba activate /mnt/vdb2/var/storage/deepfri2/toolbox/tbe
cd /mnt/vdb2/var/storage/deepfri2/toolbox/deepFRI2-toolbox-dev
```

This activates the `tbe` environment located at `/mnt/vdb2/var/storage/deepfri2/toolbox/tbe`

## Common Commands

### Running Tests
```bash
python -m pytest tests/ -v --tb=short
python -m pytest tests/test_distograms.py -v    # Run specific test file
```

### Main Toolbox Usage
```bash
python toolbox.py --help                        # Show all available commands
python toolbox.py dataset --help                # Show dataset creation help
python toolbox.py embedding --help              # Show embedding generation help
python toolbox.py input_generation --help       # Show complete pipeline help
```

### Dataset Creation Example
```bash
python toolbox.py dataset -p /path/to/data -d pdb -c protein -e esm2_t33_650M_UR50D
```

### Complete Pipeline
```bash
python toolbox.py input_generation -p /path/to/data -d pdb -c protein -e esm2_t33_650M_UR50D
```

## High-Level Architecture

### Core Components

**Main Entry Point**: `toolbox.py`
- Command-line interface with subcommands for different operations
- Handles argument parsing and delegates to appropriate modules

**Dataset Management** (`toolbox/models/manage_dataset/`)
- `structures_dataset.py`: Core dataset creation and management
- `database_type.py` & `collection_type.py`: Enum definitions for supported databases and collections
- `compute_batches.py`: Batch processing logic
- `index/`: File indexing system for tracking processed files

**Embedding System** (`toolbox/models/embedding/`)
- `embedder/base_embedder.py`: Abstract base class for all embedders
- `embedder/esm2_embedder.py` & `embedder/esmc_embedder.py`: Specific implementations
- Supports various ESM model variants for protein sequence embeddings

**Distogram Generation** (`toolbox/models/`)
- `distogram.py`: Distance matrix generation from protein coordinates
- `distogramType.py`: Type definitions for different distance calculations (CA, CB)

**Data Processing Pipeline**:
1. **Structure Download**: Downloads protein structures from databases (PDB, AlphaFold)
2. **Sequence/Coordinate Extraction**: Extracts sequences and 3D coordinates
3. **Embedding Generation**: Creates sequence embeddings using transformer models
4. **Distogram Calculation**: Computes inter-residue distance matrices

### Key Data Flow
1. Input: Protein IDs or structure files
2. Download/retrieve protein structures
3. Extract sequences and coordinates
4. Generate embeddings using ESM models
5. Calculate distograms from coordinates
6. Store results in HDF5 format with indexing

### Configuration
- `config.json`: Main configuration file with paths and parameters
- `toolbox_env_conda.yml`: Conda environment specification
- Supports both CA (alpha carbon) and CB (beta carbon) distance calculations

### Distributed Computing
- Uses Dask for distributed processing
- Supports SLURM job scheduler integration
- Batch processing for large datasets

### Logging
- **Automatic file logging** to `datasets/[NAME]/log.txt` with append behavior for reuse across runs
- **Colored output** in both console and log files (ANSI color codes preserved for modern editors)
- **Session separators** in log files to distinguish between different execution runs
- **Command line logging** - Full command with all options is logged at the start of each session
- Configurable log levels and custom log file locations
- Use `--log-file` for custom logging locations
- Use `--verbose` or `-v` for detailed logging
- Log files maintain full color formatting and can be viewed with color support in modern editors
- Each new execution logs the complete command: `Started with command: python toolbox.py [args...]`

### File Organization
- `data/`: Input and output data storage
- `tests/`: Comprehensive test suite with expected/generated data comparison
- `experiments/`: Analysis and benchmarking scripts
- `examples/`: Usage examples and tutorials