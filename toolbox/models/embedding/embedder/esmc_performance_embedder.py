"""
ESMC Performance Comparison Embedder

Concrete implementation of the performance comparison embedder using the ESMC (Evolutionary Scale Modeling Compound) model.
This embedder compares parallel batch saving vs batch-end saving strategies specifically for ESMC embeddings.
"""

import torch
from pathlib import Path
from typing import Dict
import gc

from toolbox.models.embedding.embedder.performance_comparison_embedder import PerformanceComparisonEmbedder
from toolbox.utlis.logging import logger

# Import ESMC dependencies - same as the original ESMC embedder
try:
    from esm.models.esmc import ESMC
    from esm.sdk.api import ESMProtein, LogitsConfig
    ESMC_AVAILABLE = True
except ImportError as e:
    logger.warning(f"ESMC not available: {e}")
    ESMC_AVAILABLE = False


class ESMCPerformanceEmbedder(PerformanceComparisonEmbedder):
    """
    ESMC-based performance comparison embedder.
    
    Uses the ESMC model to generate embeddings while comparing the performance
    of parallel batch saving vs batch-end saving strategies.
    """
    
    def __init__(self, device=None, batch_size=1000, enable_detailed_profiling=True, 
                 comparison_mode="both", model_name="esmc_600m"):
        """
        Initialize the ESMC performance comparison embedder.
        
        Args:
            device: PyTorch device to use for computation
            batch_size: Number of sequences to process before saving a batch (for parallel mode)
            enable_detailed_profiling: If True, enables per-sequence profiling
            comparison_mode: "both", "parallel_only", or "batch_end_only"
            model_name: ESMC model name to use
        """
        if not ESMC_AVAILABLE:
            raise ImportError("ESMC is not available. Please install the required dependencies.")
            
        super().__init__(device, batch_size, enable_detailed_profiling, comparison_mode)
        self.model_name = model_name
        self.model = None
        
        logger.info(f"Initialized ESMC Performance Embedder with model: {model_name}")
    
    def _initialize_model(self):
        """Initialize the ESMC model."""
        if self.model is not None:
            # Clean up existing model
            del self.model
            torch.cuda.empty_cache()
            gc.collect()
        
        logger.info(f"Loading ESMC model: {self.model_name}")
        
        try:
            self.model = ESMC.from_pretrained(self.model_name).to(self.device)
            self.model.eval()
            logger.info(f"ESMC model loaded successfully on {self.device}")
        except Exception as e:
            logger.error(f"Failed to load ESMC model: {e}")
            raise
    
    def get_embedding(self, prot_id: str, prot_seq: str):
        """
        Generate embedding for a protein sequence using ESMC.
        
        Args:
            prot_id: Unique identifier for the protein
            prot_seq: Protein sequence string
            
        Returns:
            np.ndarray: Embedding tensor of shape (seq_length, embedding_dim)
        """
        
        
        # Create ESM protein object
        protein = ESMProtein(sequence=prot_seq)
        
        # Encode the protein
        protein_tensor = self.model.encode(protein)
        
        # Get logits with embeddings
        logits_output = self.model.logits(
            protein_tensor, 
            LogitsConfig(sequence=True, return_embeddings=True)
        )
        
        # Extract embeddings (remove start/end tokens)
        embeddings = logits_output.embeddings[0, 1:-1, :].to('cpu').detach().to(torch.float32).numpy()
        
        # Clean up intermediate tensors
        del logits_output, protein_tensor, protein
        
        return embeddings
            
    
    def embed(self, sequences: Dict[str, str], output_path: Path) -> Dict[str, str]:
        """
        Run ESMC performance comparison between parallel batch saving and batch-end saving.
        
        Args:
            sequences: Dictionary mapping protein IDs to sequences
            output_path: Path to save embedding files and performance reports
            
        Returns:
            dict: Final index mapping protein IDs to file paths
        """
        logger.info(f"Starting ESMC performance comparison for {len(sequences)} sequences")
        logger.info(f"Model: {self.model_name}, Device: {self.device}")
        
        # Run the parent comparison
        return super().embed(sequences, output_path)
