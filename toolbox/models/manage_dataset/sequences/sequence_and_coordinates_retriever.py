from pathlib import Path
import time
import h5py
import numpy as np
from distributed import Client
from typing import Dict, List, Tuple

from toolbox.models.manage_dataset.compute_batches import ComputeBatches
from toolbox.models.manage_dataset.index.handle_index import read_index, create_index
from toolbox.models.manage_dataset.index.handle_indexes import HandleIndexes
from toolbox.models.manage_dataset.utils import format_time, read_pdbs_from_h5
from toolbox.models.utils.cif2pdb import aa_dict
from toolbox.config import CarbonAtomType
from toolbox.utlis.logging import log_title, logger


empty_chain_part_sign = "X"  # Added for sequence gaps/non-standard

def __extract_sequences_and_coordinates__(
    file: str,
    carbon_atom_type: CarbonAtomType,
) -> tuple[str, tuple[tuple[float, float, float], tuple[float | None, float | None, float | None]]]:
    ca_coords = []  # Will preserve file order
    coords_with_breaks = []
    sequence_chars = [] # For building the sequence

    # Dictionary to store coordinates by residue number (for coords_with_breaks)
    residue_coords = {}
    # Dictionary to store residue name by residue number (for sequence)
    residue_info = {}
    # Track all residue numbers that have any atoms (for determining range)
    all_residue_numbers = set()

    # Track what we've seen for the current residue being processed
    current_residue_coord = None
    current_residue_num = None # This tracks the residue number of the current ATOM line

    # This will track the residue number of the last amino acid added to sequence
    # to handle cases where ATOM records for a residue might be sparse
    # but we only want to add the amino acid once.
    # However, the refined logic below populates residue_info once per residue number
    # and then builds the sequence based on the min/max range, which is more robust.

    def process_current_residue():
        """Helper function to process the current residue data"""
        nonlocal current_residue_coord, current_residue_num

        if current_residue_num is None:
            return

        if current_residue_coord is not None:
            # Add to ca_coords in FILE ORDER (as we process them)
            ca_coords.append(current_residue_coord)
            # Store in dictionary for building coords_with_breaks later
            residue_coords[current_residue_num] = current_residue_coord
        # Note: Sequence character is added based on residue_info later,
        # not in this helper, to ensure it aligns with min/max range.


    for line in file.splitlines():
        if line.startswith("ATOM"):
            atom_type = line[12:16].strip()
            residue_num_for_atom = int(line[22:26].strip()) # Renamed to avoid clash
            residue_name = line[17:20].strip()

            # Track all residue numbers (for determining the range)
            all_residue_numbers.add(residue_num_for_atom)

            # Store residue name if this is the first time we see this residue number
            if residue_num_for_atom not in residue_info:
                residue_info[residue_num_for_atom] = residue_name

            # If we're moving to a new residue (based on atom records for specified carbon type)
            if current_residue_num is not None and residue_num_for_atom != current_residue_num:
                process_current_residue()
                # Reset for new residue (for coordinate processing)
                current_residue_coord = None

            if atom_type == carbon_atom_type:
                # Parse coordinates
                x = float(line[30:38])
                y = float(line[38:46])
                z = float(line[46:54])

                current_residue_coord = (x, y, z)

            current_residue_num = residue_num_for_atom # Update current_residue_num for coordinate processing logic

    # Process the final residue's coordinates after the loop
    process_current_residue()

    # Build sequence and coords_with_breaks only if we found any residues
    if all_residue_numbers: # Check if any ATOM lines were processed
        actual_min = min(all_residue_numbers)
        actual_max = max(all_residue_numbers)

        for res_idx in range(actual_min, actual_max + 1):
            # Build sequence
            if res_idx in residue_info:
                aa_code = aa_dict.get(residue_info[res_idx], empty_chain_part_sign)
                sequence_chars.append(aa_code)
            else:
                sequence_chars.append(empty_chain_part_sign)

            # Build coords_with_breaks (includes gaps as None tuples, in residue number order)
            if res_idx in residue_coords:
                coords_with_breaks.append(residue_coords[res_idx])
            else:
                coords_with_breaks.append((None, None, None))
    
    final_sequence = "".join(sequence_chars)
    return final_sequence, (tuple(ca_coords), tuple(coords_with_breaks))


def __get_sequences_and_coordinates_from_batch__(
    hdf_file_path: str,
    codes: List[str],
    carbon_atom_type: CarbonAtomType,
    batch_output_path: str,
) -> Tuple[List[str], Dict[str, str]]:
    """
    Extract sequences and coordinates from a batch of PDB structures.
    
    Args:
        hdf_file_path: Path to the H5 file containing PDB data
        codes: List of protein codes to process
        carbon_atom_type: Type of carbon atom to extract coordinates for ("CA" or "CB")
        batch_output_path: Path where to save the coordinates H5 file
        substitute_non_standard_aminoacids: Whether to substitute non-standard amino acids
        
    Returns:
        Tuple of (sequence_lines, mapping id->coordinates_h5_path)
    """
    proteins: Dict[str, str] = read_pdbs_from_h5(hdf_file_path, codes)
    
    if not proteins:
        return [], {}
    
    sequence_lines = []
    coordinates_data = {}
    
    for code, pdb_content in proteins.items():
        try:
            sequence, (ca_coords, coords_with_breaks) = __extract_sequences_and_coordinates__(
                pdb_content, carbon_atom_type
            )
            
            # Add sequence to FASTA format
            clean_code = code.removesuffix('.pdb')
            sequence_lines.append(f">{clean_code}\n{sequence}\n")
            
            # Store only coordinates with breaks (aligned to sequence), rename to 'coords'
            coordinates_data[clean_code] = {
                'coords': np.array(coords_with_breaks, dtype=np.float32)
            }
            
        except Exception as e:
            logger.error(f"Error processing {code}: {e}")
            continue
    
    # Save coordinates to H5 file
    coordinates_h5_path = _save_coordinates_to_h5(batch_output_path, coordinates_data)
    
    # Build mapping of protein_id -> created H5 file path
    coords_index_partial = {protein_id: coordinates_h5_path for protein_id in coordinates_data.keys()}
    
    return sequence_lines, coords_index_partial


def _save_coordinates_to_h5(output_path: str, coordinates_data: Dict[str, Dict]) -> str:
    """
    Save coordinates data to H5 file.
    
    Args:
        output_path: Base path for the output H5 file
        coordinates_data: Dictionary containing coordinates for each protein
        
    Returns:
        Path to the created H5 file
    """
    h5_path = f"{output_path}.h5"
    
    try:
        with h5py.File(h5_path, 'w') as f:
            for protein_id, coords in coordinates_data.items():
                protein_group = f.create_group(protein_id)
                
                # Save coordinates (aligned to sequence)
                if len(coords['coords']) > 0:
                    protein_group.create_dataset(
                        'coords', 
                        data=coords['coords'],
                        compression='gzip',
                        compression_opts=9
                    )
        
        logger.debug(f"Saved coordinates to {h5_path}")
        return h5_path
        
    except Exception as e:
        logger.error(f"Error saving coordinates to H5: {e}")
        return ""


class SequenceAndCoordinatesRetriever:
    """
    Retrieves both sequences and coordinates from PDB structures and saves them.
    Similar to SequenceRetriever but also extracts and saves coordinates to H5 files.
    """

    def __init__(self, structures_dataset):
        self.structures_dataset = structures_dataset
        self.handle_indexes: HandleIndexes = self.structures_dataset._handle_indexes

    def retrieve(
        self, 
        carbon_atom_type: CarbonAtomType = "CA",
    ):
        """
        Retrieve sequences and coordinates from the structures dataset.
        
        Args:
            carbon_atom_type: Type of carbon atom to extract coordinates for ("CA" or "CB")
            substitute_non_standard_aminoacids: Whether to substitute non-standard amino acids
        """
        
        log_title("Retrieving sequences and coordinates")

        start = time.time()

        structures_dataset = self.structures_dataset

        protein_index = read_index(
            structures_dataset.dataset_index_file_path(), 
            structures_dataset.config.data_path
        )
        
        search_index_result = self.handle_indexes.full_handle(
            "sequences", protein_index, structures_dataset.overwrite
        )

        h5_file_to_codes = search_index_result.grouped_missing_proteins
        missing_sequences = search_index_result.missing_protein_files.keys()
        sequences_coords_index = search_index_result.present

        logger.info("Getting sequences and coordinates from stored PDBs")

        client: Client = self.structures_dataset._client

        def run(input_data, workers):
            return client.submit(__get_sequences_and_coordinates_from_batch__, *input_data, workers=workers)
        
        # Create output directories
        sequences_path_obj = Path(self.structures_dataset.config.data_path) / "sequences"
        coordinates_path_obj = Path(self.structures_dataset.config.data_path) / "coordinates"
        
        for path_obj in [sequences_path_obj, coordinates_path_obj]:
            if not path_obj.exists():
                path_obj.mkdir(exist_ok=True, parents=True)

        # File names
        base_name = structures_dataset.dataset_dir_name()
        atom_suffix = f"_{carbon_atom_type.lower()}"
        
        sequences_file_name = f"{base_name}{atom_suffix}.fasta"
        sequences_file_path = sequences_path_obj / sequences_file_name

        # Load existing coordinates index (if any) so that we can append new entries
        coordinates_index = read_index(
            structures_dataset.coordinates_index_path(),
            structures_dataset.config.data_path,
        )

        with open(sequences_file_path, "w") as f:

            def collect(result):
                sequence_lines, coords_mapping = result
                f.writelines(sequence_lines)
                # Merge the per-protein mapping returned from the batch
                coordinates_index.update(coords_mapping)

            compute = ComputeBatches(client, run, collect, "sequences_and_coordinates")

            inputs = (
                (
                    file, 
                    codes, 
                    carbon_atom_type,
                    str(coordinates_path_obj / f"{base_name}_batch_{i}_{carbon_atom_type.lower()}"),
                )
                for i, (file, codes) in enumerate(h5_file_to_codes.items())
            )

            compute.compute(inputs)

        logger.info("Saving sequences and coordinates")
        
        # Update index with sequence file path (store as string like the original sequence_retriever)
        for id_ in missing_sequences:
            sequences_coords_index[id_] = str(sequences_file_path)
        
        # Save sequences index using the standard sequences.idx file
        sequences_index_path = structures_dataset.sequences_index_path()
        create_index(sequences_index_path, sequences_coords_index, structures_dataset.config.data_path)

        ids_to_process = list(coordinates_index.keys())
        for protein_id in ids_to_process:
            if protein_id.endswith(".pdb"):
                coordinates_index[protein_id.removesuffix(".pdb")] = coordinates_index.pop(protein_id)

        # Save / update the coordinates index on disk
        create_index(
            structures_dataset.coordinates_index_path(), 
            coordinates_index, 
            structures_dataset.config.data_path
        )

        end = time.time()
        logger.info(f"Total time: {format_time(end - start)}")
