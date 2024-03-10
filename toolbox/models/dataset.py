from typing import List
from pathlib import Path

from pydantic import BaseModel

from toolbox.models.protein.protein import Protein
from toolbox.models.proteinType import ProteinType


class Dataset(BaseModel):
    name: str
    proteins: List[Protein]
    n: int = len(proteins)
    type: ProteinType
    api_link: str
    structure_location: Path
    distance_location: Path

    def fetch_structures(self, overwrite: bool = False):
        pass

    def generate_distograms(self, overwrite: bool = False):
        pass
