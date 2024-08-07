from typing import Dict

from Bio import SeqIO

from toolbox.models.manage_dataset.handle_index import read_index
from toolbox.models.manage_dataset.sequences.from_pdb import aa_dict
from toolbox.models.manage_dataset.structures_dataset import StructuresDataset
from toolbox.models.manage_dataset.utils import read_all_pdbs_from_h5


def _parse_pdb_residue_(pdb_code, pdb_str):
    lines = pdb_str.split('\n')

    results = []
    for line in lines:
        # Check if the line starts with 'ATOM'
        if line.startswith('ATOM'):
            # Extract residue number (columns 23-26) and amino acid (columns 18-20)
            residue_number = line[22:26].strip()
            amino_acid = line[17:20].strip()

            try:
                short_acid = aa_dict[amino_acid]
                results.append((residue_number, short_acid))
            except KeyError:
                print(pdb_code)
                print(line)
                print(amino_acid)

    return dict(results)


def verify_chains(structures_dataset: StructuresDataset, pdb_seqres_fasta_path):
    proteins_index = read_index(structures_dataset.dataset_path() / 'dataset_reversed.idx')

    fasta_index = SeqIO.index(pdb_seqres_fasta_path, "fasta")

    for h5_file in proteins_index.keys():
        prots = read_all_pdbs_from_h5(h5_file)

        for p, content in prots.items():
            code = p.removesuffix('.pdb')
            acids_from_pdb: Dict[int, str] = _parse_pdb_residue_(code, content)

            try:
                seqres_sequence: str = fasta_index[code].seq
            except KeyError:
                print(f"The fasta index hasn't entry for the code {code}.")
                continue

            sequence_dict: Dict[int, str] = {i: char for i, char in enumerate(seqres_sequence, start=1)}

            for residue_num, acid in acids_from_pdb.items():
                if int(residue_num) not in sequence_dict or sequence_dict[int(residue_num)] != acid:
                    print(
                        f"Residue number {residue_num} or its corresponding acid {acid} does not match in sequence_dict for the code {code}.")

        break
