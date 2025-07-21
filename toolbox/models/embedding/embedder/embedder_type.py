from enum import Enum
from toolbox.models.embedding.embedder.esm2_embedder import ESM2Embedder
from toolbox.models.embedding.embedder.esmc_embedder import ESMCEmbedder
from toolbox.models.embedding.embedder.base_embedder import BaseEmbedder

class EmbedderType(Enum):
    ESM2 = ("esm2_t33_650M_UR50D", ESM2Embedder)
    ESMC = ("esmc_600m", ESMCEmbedder) 

    def __init__(self, value, embedder_class: BaseEmbedder):
        self._value_ = value
        self.embedder_class: BaseEmbedder = embedder_class