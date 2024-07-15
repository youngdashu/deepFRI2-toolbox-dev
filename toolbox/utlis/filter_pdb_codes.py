from typing import TextIO, List, Set, Iterable


def _read_pdb_types_(file: TextIO) -> Set[str]:
    valid_types = {'prot', 'prot-nuc'}

    allowed_pdbs_set = {parts[0].decode() for line in file if
                        len(parts := line.strip().split()) >= 2 and parts[1].decode() in valid_types}

    return allowed_pdbs_set


def filter_pdb_codes(pdb_mol_types_file: TextIO, pdb_codes: Iterable[str]) -> Set[str]:
    allowed_pdbs_set = _read_pdb_types_(pdb_mol_types_file)
    return set(pdb_codes) & allowed_pdbs_set
