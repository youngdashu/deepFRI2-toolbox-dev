from typing import List
from pathlib import Path

from pydantic import BaseModel

from toolbox.models.protein.protein import Protein
from toolbox.models.proteinType import ProteinType


class Dataset(BaseModel):
    name: str
    proteins: List[Protein]
    n_proteins: int = len(proteins)
    type: ProteinType
    n_batches: int # number of batches max 1k-10k proteins and max 10k subfolders
    api_link: str
    struct_location: Path
    disto_location: Path

    def fetch_structures(self, overwrite: bool = False):
        pass

    def generate_distograms(self, overwrite: bool = False):
        pass
