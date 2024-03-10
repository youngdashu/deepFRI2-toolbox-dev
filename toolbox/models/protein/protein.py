from pathlib import Path
from typing import List, Union

from pydantic import BaseModel

from toolbox.models.distogram import Distogram
from toolbox.models.proteinType import ProteinType

class Sequence(BaseModel):
    amino_acids: str
    header: str

class Structure(BaseModel):
    atom_coordinates: List[List[float]]

class Metadata(BaseModel):
    origin: str
    proteinType: ProteinType
    location: Path

class Protein(BaseModel):
    name: str
    sequence: Sequence
    structure: Structure
    metadata: Metadata
    annotation: str
    distogram: Distogram



    # def __init__(self, name: str, sequence: str, header: str, structure: List, metadata: dict, origin: str, protein_type: ProteinType, location: Path, annotation: Union[callable, None] = None):
    #     self.name = name
    #     self.sequence = sequence
    #     self.amino_acids = sequence  # Assuming sequence is a string of amino acids
    #     self.header = header
    #     self.structure = structure
    #     self.metadata = metadata
    #     self.origin = origin
    #     self.type = protein_type
    #     self.location = location
    #     self.annotation = annotation if annotation is not None else self.default_annotation
    #     self.distogram = None  # Placeholder, as generating a distogram would require further details

    def default_annotation(self):
        # Placeholder method for annotation
        pass








