# CSV Range Processing Documentation

## Overview

The DeepFRI2 Toolbox now supports advanced CSV input files that can specify protein chains and residue ranges for precise extraction during processing. This feature allows you to process only specific parts of proteins, which is particularly useful for large protein complexes or when you need to focus on specific domains.

## CSV Format Specification

### Simple Format (Legacy)
```csv
protein_id
AF-A0A0A0K0K1-F1-model_v4
AF-A0A024B7W1-F1-model_v4
```

### Complex Format (New)
```csv
AF-A0A0A0K0K1-F1-model_v4__nan,
AF-A0A024B7W1-F1-model_v4__8cxi_C,1-403
AF-A0A024B7W1-F1-model_v4__8cxi_D,1-403
AF-A0A024B7W1-F1-model_v4__8cxi_E,202-613;700-712
```

### Format Components

#### 1. Protein ID Structure
- **Base ID**: Core protein identifier (e.g., `AF-A0A024B7W1-F1-model_v4`)
- **Chain Separator**: Double underscore (`__`) separates base ID from chain identifier
- **Chain ID**: Specific chain or domain identifier (e.g., `8cxi_C`)
- **Special Values**: Use `nan` for the entire protein without chain specification

#### 2. Range Specification
- **Format**: `start-end` where start and end are residue numbers (inclusive)
- **Multiple Ranges**: Separate multiple ranges with semicolons (`;`)
- **Examples**:
  - Single range: `1-403`
  - Multiple ranges: `202-613;700-712`
  - No ranges: Leave empty after comma or omit comma entirely

#### 3. Complete Entry Format
```
<base_protein_id>[__<chain_id>][,<range1-range2>[;<range3-range4>...]]
```

## Processing Logic

### 1. Format Detection
The system automatically detects whether a CSV file uses simple or complex format by checking for:
- Presence of double underscore (`__`) in entries
- Presence of commas indicating range specifications

### 2. Deduplication Rules
- **Identical Ranges**: If multiple protein entries have identical range specifications, only the first one is processed
- **Example**: In the sample above, both `8cxi_C` and `8cxi_D` have range `1-403`, so only `8cxi_C` is kept

### 3. Range Processing
- **Inclusive Boundaries**: Both start and end residue numbers are included
- **Multiple Ranges**: All specified ranges are extracted and combined
- **Gap Handling**: Residues between ranges are filled with gap characters (`X`) in sequences and `None` values in coordinates

## Usage Examples

### 1. Dataset Creation with Range CSV
```bash
python toolbox.py dataset -p /path/to/data -d pdb -c subset -i complex_proteins.csv -e esm2_t33_650M_UR50D
```

### 2. Complete Pipeline with Ranges
```bash
python toolbox.py input_generation -p /path/to/data -d pdb -c subset -i complex_proteins.csv -e esm2_t33_650M_UR50D
```

### 3. CSV File Examples

#### Example 1: Mixed Whole and Partial Proteins
```csv
AF-A0A0A0K0K1-F1-model_v4__nan,
AF-A0A024B7W1-F1-model_v4__8cxi_C,1-403
AF-Q9Y6K5-F1-model_v4__domain1,150-350
AF-Q9Y6K5-F1-model_v4__domain2,400-600;650-750
```

#### Example 2: Protein Complex Chains
```csv
1abc__A,1-250
1abc__B,1-180
1abc__C,1-320
1def__A,50-200
1def__B,75-225
```

## Integration with Existing Features

### 1. Sequence Extraction
- Range filtering is applied during PDB/CIF parsing
- Only residues within specified ranges are included in output sequences
- Gap characters maintain alignment with coordinate data

### 2. Coordinate Extraction
- Coordinates are extracted only for residues within specified ranges
- Coordinate arrays maintain residue number indexing
- Gap positions contain `None` values for x, y, z coordinates

### 3. Embedding Generation
- Sequences with range filtering are processed normally by embedding models
- Gap characters are handled according to the embedding model's specifications

### 4. Distogram Generation
- Distance matrices are calculated only for residues present after range filtering
- Matrix dimensions correspond to the filtered sequence length

## File Structure and Output

### 1. Processed Identifiers
Complex CSV entries are processed into clean identifiers:
- Input: `AF-A0A024B7W1-F1-model_v4__8cxi_C,1-403`
- Output ID: `AF-A0A024B7W1-F1-model_v4__8cxi_C`

### 2. Output Files
All output files use the processed identifier format:
- **Sequences**: `>AF-A0A024B7W1-F1-model_v4__8cxi_C` in FASTA files
- **Coordinates**: Stored under the same identifier in HDF5 files
- **Embeddings**: Generated using the processed identifier

### 3. Chain Mapping
The system handles mapping between different chain identifier formats:
- **CSV Format**: `protein__chain` (double underscore)
- **PDB Format**: `protein_chain` (single underscore)
- **Automatic Conversion**: System converts between formats as needed

## Error Handling

### 1. Invalid Range Specifications
```csv
AF-A0A024B7W1-F1-model_v4__8cxi_C,invalid-range  # Skipped with warning
AF-A0A024B7W1-F1-model_v4__8cxi_D,403-1          # Error: start > end
AF-A0A024B7W1-F1-model_v4__8cxi_E,abc-def        # Error: non-integer values
```

### 2. Missing Residues
- If specified ranges extend beyond available residues, only available residues are extracted
- Empty results are returned if no residues match the specified ranges

### 3. Malformed Entries
- Malformed CSV entries are skipped with warnings
- Processing continues with remaining valid entries

## Migration Guide

### From Simple to Complex Format

**Old CSV:**
```csv
protein_id
AF-A0A024B7W1-F1-model_v4
```

**New CSV (equivalent):**
```csv
AF-A0A024B7W1-F1-model_v4__nan,
```

**New CSV (with specific chain and range):**
```csv
AF-A0A024B7W1-F1-model_v4__8cxi_C,1-403
```

### Backward Compatibility
- Simple CSV files continue to work without modification
- No changes required for existing workflows using simple CSV format
- Complex features are opt-in through new CSV format

## Performance Considerations

### 1. Processing Efficiency
- Range filtering reduces memory usage for large proteins
- Smaller coordinate arrays improve processing speed
- Embedding generation time is reduced for shorter sequences

### 2. Storage Optimization
- Only required residue data is stored
- Reduced file sizes for coordinate and embedding data
- More efficient disk usage for large datasets

## Troubleshooting

### Common Issues

1. **No Output Generated**
   - Check that ranges overlap with available residue numbers
   - Verify CSV format syntax (commas, semicolons, dashes)
   - Ensure protein IDs exist in the specified database

2. **Unexpected Deduplication**
   - Review range specifications for identical patterns
   - Check that intended ranges are actually different
   - Consider reordering CSV entries if first occurrence matters

3. **Chain Mapping Issues**
   - Verify chain identifiers match those in PDB/CIF files
   - Check that double underscore format is used correctly
   - Ensure chain IDs are valid for the specified protein

### Debugging Tips

1. **Enable Verbose Logging**
   ```bash
   python toolbox.py dataset -v -i complex_proteins.csv ...
   ```

2. **Check Generated Identifiers**
   - Review log output for processed protein IDs
   - Verify deduplication behavior matches expectations

3. **Validate Range Specifications**
   - Use small test CSV files to verify range processing
   - Check output sequence lengths match expected ranges

## API Reference

### CSVProcessor Class

```python
from toolbox.models.manage_dataset.csv_processor import CSVProcessor, ProteinEntry

# Initialize processor
processor = CSVProcessor(Path("proteins.csv"))

# Extract simple IDs
ids = processor.extract_ids()

# Extract complex IDs with deduplication
ids = processor.extract_complex_protein_ids()

# Get full entries with range information
entries = processor.get_protein_entries()

# Access individual entry properties
for entry in entries:
    print(f"Base ID: {entry.base_id}")
    print(f"Chain: {entry.chain}")
    print(f"Ranges: {entry.ranges}")
    print(f"Full ID: {entry.full_id}")
```

### ProteinEntry Class

```python
from toolbox.models.manage_dataset.csv_processor import ProteinEntry

# Create entry
entry = ProteinEntry(
    base_id="AF-A0A024B7W1-F1-model_v4",
    chain="8cxi_C", 
    ranges=[(1, 403), (500, 600)]
)

# Access properties
print(entry.full_id)  # "AF-A0A024B7W1-F1-model_v4__8cxi_C"
print(entry.ranges)   # [(1, 403), (500, 600)]
```