from pathlib import Path

from Bio.PDB import PDBParser
from Bio.SeqUtils import seq1


def get_sequence_from_pdbs(file_path: str):
    pdb_parser = PDBParser()

    # print(file_paths)
    path = Path(file_path)
    structure = pdb_parser.get_structure("", path)
    chains = {chain.id: seq1(''.join(residue.resname for residue in chain)) for chain in structure.get_chains()}

    protein_name = path.stem

    return {protein_name: chains}
