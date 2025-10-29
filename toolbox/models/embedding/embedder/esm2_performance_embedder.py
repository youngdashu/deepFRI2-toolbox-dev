from typing import Dict  
from pathlib import Path  
import torch  
import tqdm  
import h5py  
import gc  # Added for garbage collection  
from transformers import AutoTokenizer, AutoModelForMaskedLM  
from multiprocessing import Process
from toolbox.models.embedding.utils import save_batch
from .base_embedder import BaseEmbedder

from toolbox.models.embedding.embedder.performance_comparison_embedder import PerformanceComparisonEmbedder

from toolbox.utlis.logging import logger
# Parameters  
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

class ESM2PerformanceEmbedder(PerformanceComparisonEmbedder):
    def __init__(self, device=None, batch_size=1000, model_name='esm2_t33_650M_UR50D'):
        super().__init__(device, batch_size, "both")
        self.model_name = model_name
        self.tokenizer = AutoTokenizer.from_pretrained(f"facebook/{model_name}")
        self.model = AutoModelForMaskedLM.from_pretrained(f"facebook/{model_name}").to(self.device).to(torch.float32)
        

    def get_embedding(self, prot_id, prot_seq):
        inputs = self.tokenizer(prot_seq, return_tensors="pt")
        inputs = {k: v.to(self.device) for k, v in inputs.items()}
        outputs = self.model(**inputs, output_hidden_states=True)
        embeddings = outputs.hidden_states[-1]
        embeddings = embeddings.detach().to('cpu', non_blocking=True)[0, 1:-1] 
        embeddings = embeddings.to(torch.float32).numpy() if embeddings.dtype != torch.float32 else embeddings.numpy()
        return embeddings

    def get_embedding_profiled(self, prot_id, prot_seq):
        import time
        
        timing = {}
        
        # Time tokenization
        t0 = time.perf_counter()
        inputs = self.tokenizer(prot_seq, return_tensors="pt")
        t1 = time.perf_counter()
        timing["tokenization"] = t1 - t0
        
        # Time device transfer
        t0 = time.perf_counter() 
        inputs = {k: v.to(self.device) for k, v in inputs.items()}
        t1 = time.perf_counter()
        timing["to_device"] = t1 - t0
        
        # Time model inference
        t0 = time.perf_counter()
        outputs = self.model(**inputs, output_hidden_states=True)
        embeddings = outputs.hidden_states[-1]
        t1 = time.perf_counter()
        timing["inference"] = t1 - t0
        
        # Time post-processing
        t0 = time.perf_counter()
        embeddings = embeddings.detach().to('cpu', non_blocking=True)[0, 1:-1] 
        t1 = time.perf_counter()
        timing["post_processing_get_embeddings"] = t1 - t0
        t0 = time.perf_counter()
        embeddings = embeddings.to(torch.float32).numpy() if embeddings.dtype != torch.float32 else embeddings.numpy()
        t1 = time.perf_counter()
        timing["post_processing_convert_to_numpy"] = t1 - t0
        
        return embeddings, timing

    def _initialize_model(self):
        if self.model is not None:
            del self.model
            torch.cuda.empty_cache()
            gc.collect()

        logger.info(f"Loading ESM2 model: {self.model_name}")

        self.model = AutoModelForMaskedLM.from_pretrained(f"facebook/{self.model_name}").to(self.device).to(torch.float32)
        self.model.eval()
        logger.info(f"ESM2 model loaded successfully on {self.device}")