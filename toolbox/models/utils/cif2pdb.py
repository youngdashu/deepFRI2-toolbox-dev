from io import BytesIO
from typing import Dict, Literal, Tuple, List, Union, Optional

import biotite.structure.io.pdbx
import biotite.structure.io.pdbx.bcif as bcif

LOOP_ID = "loop_"
LOOP_EL_ID = "_atom_site."
ATOM_ID = "ATOM"
HETATM_ID = "HETATM"

# Dictionary keys that specify indices for fields in the CIF file
KEY_RECORD = "_atom_site.group_PDB"  # record name
KEY_SERIAL = "_atom_site.id"  # atom serial number
KEY_ATOM = "_atom_site.label_atom_id"  # atom name
KEY_ALTLOC = "_atom_site.label_alt_id"  # alternate location indicator
KEY_RES = "_atom_site.label_comp_id"  # residue name
KEY_CHAIN = "_atom_site.auth_asym_id"  # strand ID / chain ID (AUTH)
KEY_RESSEQ = "_atom_site.label_seq_id"  # residue sequence number
KEY_ICODE = "_atom_site.pdbx_PDB_ins_code"  # code for insertion of residues
KEY_POS_X = "_atom_site.Cartn_x"  # orthogonal coordinate for X in [A]
KEY_POS_Y = "_atom_site.Cartn_y"  # orthogonal coordinate for Y in [A]
KEY_POS_Z = "_atom_site.Cartn_z"  # orthogonal coordinate for X in [A]
KEY_OCC = "_atom_site.occupancy"  # occupancy
KEY_TFACTOR = "_atom_site.B_iso_or_equiv"  # temperature factor
KEY_SYMBOL = "_atom_site.type_symbol"  # element symbol, right-justified
KEY_CHARGE = "_atom_site.pdbx_formal_charge"  # charge on the atom
KEY_MODEL_NUM = "_atom_site.pdbx_PDB_model_num"  # model number

# https://github.com/openmm/pdbfixer/blob/master/pdbfixer/pdbfixer.py
substitutions = {
    '2AS':'ASP', '3AH':'HIS', '5HP':'GLU', 'ACL':'ARG', 'AGM':'ARG', 'AIB':'ALA', 'ALM':'ALA', 'ALO':'THR', 'ALY':'LYS', 'ARM':'ARG',
    'ASA':'ASP', 'ASB':'ASP', 'ASK':'ASP', 'ASL':'ASP', 'ASQ':'ASP', 'AYA':'ALA', 'BCS':'CYS', 'BHD':'ASP', 'BMT':'THR', 'BNN':'ALA',
    'BUC':'CYS', 'BUG':'LEU', 'C5C':'CYS', 'C6C':'CYS', 'CAS':'CYS', 'CCS':'CYS', 'CEA':'CYS', 'CGU':'GLU', 'CHG':'ALA', 'CLE':'LEU', 'CME':'CYS',
    'CSD':'ALA', 'CSO':'CYS', 'CSP':'CYS', 'CSS':'CYS', 'CSW':'CYS', 'CSX':'CYS', 'CXM':'MET', 'CY1':'CYS', 'CY3':'CYS', 'CYG':'CYS',
    'CYM':'CYS', 'CYQ':'CYS', 'DAH':'PHE', 'DAL':'ALA', 'DAR':'ARG', 'DAS':'ASP', 'DCY':'CYS', 'DGL':'GLU', 'DGN':'GLN', 'DHA':'ALA',
    'DHI':'HIS', 'DIL':'ILE', 'DIV':'VAL', 'DLE':'LEU', 'DLY':'LYS', 'DNP':'ALA', 'DPN':'PHE', 'DPR':'PRO', 'DSN':'SER', 'DSP':'ASP',
    'DTH':'THR', 'DTR':'TRP', 'DTY':'TYR', 'DVA':'VAL', 'EFC':'CYS', 'FLA':'ALA', 'FME':'MET', 'GGL':'GLU', 'GL3':'GLY', 'GLZ':'GLY',
    'GMA':'GLU', 'GSC':'GLY', 'HAC':'ALA', 'HAR':'ARG', 'HIC':'HIS', 'HIP':'HIS', 'HMR':'ARG', 'HPQ':'PHE', 'HTR':'TRP', 'HYP':'PRO',
    'IAS':'ASP', 'IIL':'ILE', 'IYR':'TYR', 'KCX':'LYS', 'LLP':'LYS', 'LLY':'LYS', 'LTR':'TRP', 'LYM':'LYS', 'LYZ':'LYS', 'MAA':'ALA', 'MEN':'ASN',
    'MHS':'HIS', 'MIS':'SER', 'MLE':'LEU', 'MPQ':'GLY', 'MSA':'GLY', 'MSE':'MET', 'MVA':'VAL', 'NEM':'HIS', 'NEP':'HIS', 'NLE':'LEU',
    'NLN':'LEU', 'NLP':'LEU', 'NMC':'GLY', 'OAS':'SER', 'OCS':'CYS', 'OMT':'MET', 'PAQ':'TYR', 'PCA':'GLU', 'PEC':'CYS', 'PHI':'PHE',
    'PHL':'PHE', 'PR3':'CYS', 'PRR':'ALA', 'PTR':'TYR', 'PYX':'CYS', 'SAC':'SER', 'SAR':'GLY', 'SCH':'CYS', 'SCS':'CYS', 'SCY':'CYS',
    'SEL':'SER', 'SEP':'SER', 'SET':'SER', 'SHC':'CYS', 'SHR':'LYS', 'SMC':'CYS', 'SOC':'CYS', 'STY':'TYR', 'SVA':'SER', 'TIH':'ALA',
    'TPL':'TRP', 'TPO':'THR', 'TPQ':'ALA', 'TRG':'LYS', 'TRO':'TRP', 'TYB':'TYR', 'TYI':'TYR', 'TYQ':'TYR', 'TYS':'TYR', 'TYY':'TYR'
}
proteinResidues = ['ALA', 'ASN', 'CYS', 'GLU', 'HIS', 'LEU', 'MET', 'PRO', 'THR', 'TYR', 'ARG', 'ASP', 'GLN', 'GLY', 'ILE', 'LYS', 'PHE', 'SER', 'TRP', 'VAL']
rnaResidues = ['A', 'G', 'C', 'U', 'I']
dnaResidues = ['DA', 'DG', 'DC', 'DT', 'DI']

unwanted_residues = rnaResidues + dnaResidues

aa_dict = {
    'ALA': 'A', 'CYS': 'C', 'ASP': 'D', 'GLU': 'E', 'PHE': 'F',
    'GLY': 'G', 'HIS': 'H', 'ILE': 'I', 'LYS': 'K', 'LEU': 'L',
    'MET': 'M', 'ASN': 'N', 'PRO': 'P', 'GLN': 'Q', 'ARG': 'R',
    'SER': 'S', 'THR': 'T', 'VAL': 'V', 'TRP': 'W', 'TYR': 'Y',
    'UNK': 'X'
}


def _fetch_atoms_from_cif(protein_code: str, row_type: Literal['A', 'H', 'AH'], cif_str: str) -> (
        Tuple)[List[str], Dict[str, int]]:
    """
    Fetch atoms from mmCIF file for specific chain.

    Parameters
    ----------
    protein_code : str
        4-letter protein identification
    row_type : str
        if 'A': fetch ATOM rows
        if 'H': fetch HETATM rows
        if 'AH': fetch ATOM and HETATM rows

    Returns
    -------
    list of str
        lines with atoms details in mmCIF format
    dict (str : int)
        key: field name
        value: index where to look for specific field
    """

    # assert chain, "{0} ERROR: chain not provided".format(protein_code)
    assert row_type in ["A", "H", "AH"], \
        "{0} ERROR: Row type different from A, H or AH".format(row_type)

    # chains = []
    # if chain_type in ["1", "3"]:
    #     chains = [chain]
    # elif chain_type == "2":
    #     chains = list(chain)

    atoms = []

    data = cif_str.split("\n")
    is_atom_loop = False
    is_atom_flag = False
    val_modelnum_first = None

    for i, line in enumerate(data):
        if not is_atom_loop:
            # if atom loop reached
            # create dictionary
            if line.startswith(LOOP_ID) and \
                    data[i + 1].startswith(LOOP_EL_ID):
                fields = {}
                num = 0
                is_atom_loop = True
            continue

        # if atom flag not yet reached
        # fill the dictionary
        if not is_atom_flag:
            key = line.strip("\n").strip()
            fields[key] = num
            num += 1
            if data[i + 1].startswith(ATOM_ID) or \
                    data[i + 1].startswith(HETATM_ID):
                is_atom_flag = True
                assert KEY_CHAIN in fields, "{0} ERROR: " \
                                            "chain key not found.".format(protein_code)
                # index where to look for chain ID
                # idx_chain = fields[KEY_CHAIN]
                # index where to look for model num (shall be 1)
                idx_modelnum = fields[KEY_MODEL_NUM]
                # value for the first encountered model num
                val_modelnum_first = data[i + 1].split()[idx_modelnum]
            continue

        # go through all atoms and fetch atoms
        # belonging to the chain(s)
        line_start_atom = line.startswith(ATOM_ID)
        line_start_hetm = line.startswith(HETATM_ID)
        line_start_atom_hetm = line_start_atom or line_start_hetm

        if line_start_atom_hetm:
            if (row_type == "AH" and line_start_atom_hetm) or \
                    (row_type == "A" and line_start_atom) or \
                    (row_type == "H" and line_start_hetm):
                line_splitted = line.split()
                if line_splitted[idx_modelnum] == val_modelnum_first:
                    atoms.append(line.strip())
        else:
            break

    try:
        return atoms, fields
    except UnboundLocalError as e:
        return None, None


def _create_pdb_atoms_from_cif(cif_atoms, cif_fields, identifier, with_acids_validation=False) -> Optional[List[str]]:
    """
    Transform mmCIF atoms into pdb atoms.

    Parameters
    ----------
    cif_atoms : list of str
        lines with atoms details in CIF format
    cif_fields : dict (str : int)
        key: field name
        value: index where to look for specific field
    identifier : str
        nameCHAIN - used for logging

    Returns
    -------
    list of str
        lines with atoms details in PDB format
    """
    pdb_atoms = []

    # Indices
    idx_record = cif_fields[KEY_RECORD]
    # idx_serial = cif_fields[KEY_SERIAL]
    idx_atom = cif_fields[KEY_ATOM]
    idx_altloc = cif_fields[KEY_ALTLOC]
    idx_res = cif_fields[KEY_RES]
    idx_chain = cif_fields[KEY_CHAIN]
    idx_resseq = cif_fields[KEY_RESSEQ]
    idx_icode = cif_fields[KEY_ICODE]
    idx_pos_x = cif_fields[KEY_POS_X]
    idx_pos_y = cif_fields[KEY_POS_Y]
    idx_pos_z = cif_fields[KEY_POS_Z]
    idx_occ = cif_fields[KEY_OCC]
    idx_tfactor = cif_fields[KEY_TFACTOR]
    idx_symbol = cif_fields[KEY_SYMBOL]
    idx_charge = cif_fields[KEY_CHARGE]

    # Below we save only atoms belonging to the first
    # encountered conformation (if exists)
    first_conf = None  # first conformation
    is_first_conf = False  # whether first conformation is encountered

    for i, atom in enumerate(cif_atoms):
        elements = atom.split()
        assert len(elements) == len(cif_fields), \
            "{0} ERROR: wrong number of fields for atom at position {1}".format(identifier, i + 1)

        # Preprocessing
        if elements[idx_altloc] == '.':
            elements[idx_altloc] = ""
        else:
            if not is_first_conf:
                is_first_conf = True
                first_conf = elements[idx_altloc]
            else:
                if elements[idx_altloc] != first_conf:
                    continue

        if elements[idx_icode] == '?':
            elements[idx_icode] = ""
        if elements[idx_charge] == '?':
            elements[idx_charge] = ""
        elements[idx_atom] = elements[idx_atom] \
            .replace('\'', "").replace('\"', "")

        # Create and save line

        if len(str(elements[idx_res])) < 3:
            return None

        if aa_dict.get(f"{elements[idx_res]:>3}", None) is None:
            return None

        line = f"{elements[idx_record]:<6}" \
               f"{str(i + 1)[:5]:>5}" \
               f" " \
               f"{elements[idx_atom]:^4}" \
               f" " \
               f"{elements[idx_res]:>3}" \
               f" " \
               f"{elements[idx_chain][-1]:>1}" \
               f"{elements[idx_resseq][-4:]:>4}" \
               f"{elements[idx_icode]:>1}" \
               f"   " \
               f"{elements[idx_pos_x][:8]:>8}" \
               f"{elements[idx_pos_y][:8]:>8}" \
               f"{elements[idx_pos_z][:8]:>8}" \
               f"{elements[idx_occ][:6]:>6}" \
               f"{elements[idx_tfactor][:6]:>6}" \
               f"          " \
               f"{elements[idx_symbol]:>2}" \
               f"{elements[idx_charge]:>1}" \
               f"\n"

        pdb_atoms.append(line)

    return pdb_atoms


def cif_to_pdb(cif: str, pdb_code: str) -> Dict[str, str]:
    all_atoms, fields = _fetch_atoms_from_cif(pdb_code, 'A', cif)

    if all_atoms is None or fields is None:
        return None

    # split atoms by auth_asym_id field
    chain_id_field_number = fields[KEY_CHAIN]
    atoms_per_chain = dict()
    for line in all_atoms:
        chain_id = line.split()[chain_id_field_number]
        atoms_per_chain.setdefault(chain_id, []).append(line)

    result = {}
    # write each chain to pdb, with chainId[-1] as chainId
    # return dict[pdb_code_ORIGINAL_chain, pdb_file_content]
    for chain_id, chain_atoms in atoms_per_chain.items():
        pdb_atoms = _create_pdb_atoms_from_cif(chain_atoms, fields, chain_id)
        if pdb_atoms is None:
            continue
        pdb_str = "".join(pdb_atoms)
        result[f"{pdb_code}_{chain_id}.pdb"] = pdb_str

    return result


def parse_atom_data(atom_data, occupancy=None, temp_factor=None):
    pdb_lines = []
    serial_number = 1  # Start serial numbering from 1

    # Default values if not provided
    if occupancy is None:
        occupancy = [1.00] * len(atom_data.splitlines())
    if temp_factor is None:
        temp_factor = [0.00] * len(atom_data.splitlines())

    occ_index = 0  # Index for occupancy and temp factor arrays

    for line in atom_data.splitlines():
        parts = line.split()

        if parts[0] == "HET":
            continue

        # Extract information
        chain_id = parts[0]
        residue_number = parts[1]
        residue_name = parts[2].strip()
        if residue_name in unwanted_residues:
            continue
        residue_name = substitutions.get(residue_name, residue_name)
        residue_name = aa_dict.get(residue_name)

        atom_name = parts[3]
        element_symbol = parts[4]
        x = parts[5]
        y = parts[6]
        z = parts[7]

        # Get occupancy and temp factor for the current atom
        occ = occupancy[occ_index]
        temp = temp_factor[occ_index]
        occ_index += 1

        # Format the PDB line
        # pdb_line = f"ATOM  {serial_number:>5}  {atom_name:<4}{residue_name} {chain_id[-1]}{residue_number:>4}    {x:>8.3f}{y:>8.3f}{z:>8.3f}  {occ:>5.2f}  {temp:>5.2f}           {element_symbol:>2}"

        try:
            int(residue_number[-4:])
        except Exception as e:
            continue

        pdb_line = f"ATOM  " \
               f"{str(serial_number)[-5:]:>5}" \
               f" " \
               f"{atom_name:^4}" \
               f" " \
               f"{residue_name:>3}" \
               f" " \
               f"{chain_id[-1]:>1}" \
               f"{residue_number[-4:]:>4}" \
               f"    " \
               f"{x[:8]:>8}" \
               f"{y[:8]:>8}" \
               f"{z[:8]:>8}" \
               f"{str(occ)[:6]:>6}" \
               f"{str(temp)[:6]:>6}" \
               f"          " \
               f"{element_symbol:>2}" \

        pdb_lines.append((chain_id, pdb_line))  # Store with chain_id

        # Increment the serial number
        serial_number += 1

    return pdb_lines


def split_by_chain(pdb_lines) -> Dict[str, str]:
    chain_dict = {}

    for chain_id, pdb_line in pdb_lines:
        if chain_id not in chain_dict:
            chain_dict[chain_id] = []
        chain_dict[chain_id].append(pdb_line)

    for k in chain_dict:
        chain_dict[k] = '\n'.join(chain_dict[k])

    return chain_dict


def binary_cif_to_pdb(cif_bytes: BytesIO, pdb_code: str) -> Dict[str, str]:
    b_f = bcif.BinaryCIFFile.read(cif_bytes)

    occupancies = b_f.block['atom_site']["occupancy"].as_array(float)
    b_factor = b_f.block['atom_site']["B_iso_or_equiv"].as_array(float)

    model_nums_iter = iter(set(b_f.block['atom_site']["pdbx_PDB_model_num"].as_array(int)))

    def get_struct():
        try:
            model_num = next(model_nums_iter)
            return biotite.structure.io.pdbx.get_structure(b_f, model_num)
        except ValueError as e:
            print(pdb_code, model_num, e)
            return get_struct()
        except StopIteration:
            return None

    stack = get_struct()
    if stack is None:
        return {}

    all_pdbs = str(stack)

    chain_pdbs = parse_atom_data(
        all_pdbs,
        occupancies,
        b_factor
    )

    atoms_per_chain = split_by_chain(chain_pdbs)

    result = {}
    for chain_id, pdb_atoms in atoms_per_chain.items():
        if pdb_atoms is None or len(pdb_atoms) == 0:
            continue
        result[f"{pdb_code}_{chain_id}.pdb"] = pdb_atoms

    return result
