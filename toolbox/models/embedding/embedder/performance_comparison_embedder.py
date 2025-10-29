"""
Performance Comparison Embedder

This module provides a specialized embedder that compares the performance of two different
saving strategies:
1. Parallel batch saving - saves batches as they are generated (current approach)
2. Batch-end saving - accumulates all embeddings and saves them in one operation at the end

The embedder provides comprehensive timing analysis and metrics to compare the efficiency
of both approaches.
"""

from abc import abstractmethod
from pathlib import Path
from multiprocessing import Process
from contextlib import nullcontext
import torch
import gc
import time
import json
import os
import threading
from typing import Dict, List, Tuple, Any
import h5py

from toolbox.models.embedding.embedder.base_embedder import BaseEmbedder
from toolbox.models.embedding.utils_profiled import (
    create_shared_memory_batch,
    save_batch_profiled_from_shared_memory,
)
from toolbox.utlis.logging import logger


def save_all_embeddings_at_end(output_path: Path, all_embeddings: Dict[str, torch.Tensor]) -> Dict[str, Any]:
    """
    Save all embeddings in a single operation at the end.
    
    Args:
        output_path: Directory to save the embeddings
        all_embeddings: Dictionary mapping protein IDs to their embedding tensors
    
    Returns:
        dict: Time performance metrics for the operation
    """
    start_time = time.perf_counter()
    output_file = output_path / "all_embeddings.h5"
    
    # Initialize timing log entry
    log_entry = {
        "strategy": "batch_end_save",
        "num_sequences": len(all_embeddings),
        "start_time": start_time
    }
    
    try:
        # Time the file writing process
        with h5py.File(output_file, 'w') as f:
            for seq_id, embedding in all_embeddings.items():
                f.create_dataset(seq_id, data=embedding)
            f.flush()
            
        end_time = time.perf_counter()
        total_duration = end_time - start_time
        
        # Calculate time performance metrics
        log_entry.update({
            "end_time": end_time,
            "total_duration": total_duration,
            "sequences_per_second": len(all_embeddings) / total_duration if total_duration > 0 else 0,
            "success": True
        })
        
    except Exception as e:
        error_time = time.perf_counter()
        log_entry.update({
            "end_time": error_time,
            "total_duration": error_time - start_time,
            "success": False,
            "error": str(e)
        })
        raise
    
    return log_entry


class PerformanceComparisonEmbedder(BaseEmbedder):
    """
    Performance comparison embedder that tests both parallel batch saving and batch-end saving strategies.
    
    This embedder runs the embedding process twice using different saving strategies:
    1. Parallel batch saving (saves batches as they are generated in parallel processes)
    2. Batch-end saving (accumulates all embeddings and saves them at the end)
    
    It provides comprehensive performance analysis and comparison between the two approaches.
    """
    
    def __init__(self, device=None, batch_size=1000, comparison_mode="both"):
        """
        Initialize the performance comparison embedder.
        
        Args:
            device: PyTorch device to use for computation
            batch_size: Number of sequences to process before saving a batch (for parallel mode)
            enable_detailed_profiling: If True, enables per-sequence profiling
            comparison_mode: "both", "parallel_only", or "batch_end_only"
        """
        # OVERWRITE_BATCH_SIZE = 100
        super().__init__(device, batch_size)
        self.comparison_mode = comparison_mode
        self.model = None  # Will be set by concrete implementations
        
    @abstractmethod
    def get_embedding(self, prot_id, prot_seq):
        """
        Abstract method to generate embeddings for a protein sequence.
        Must be implemented by concrete embedder classes.
        """
        pass
    
    @abstractmethod
    def _initialize_model(self):
        """
        Abstract method to initialize the embedding model.
        Must be implemented by concrete embedder classes.
        """
        pass

    def _run_parallel_process_parall_save_batch_embedding(self, sequences: Dict[str, str], output_path: Path) -> Tuple[Dict[str, str], Dict[str, Any]]:
        """
        Run embedding process with parallel batch saving strategy.
        Processes batches on main thread and saves to h5 in separate processes.
        
        Args:
            sequences: Dictionary mapping protein IDs to sequences
            output_path: Path to save embedding batch files
        """
        parallel_output_path = output_path / "parallel_batches"
        parallel_output_path.mkdir(exist_ok=True, parents=True)
        
        parallel_start_time = time.perf_counter()
        performance_metrics = {
            "strategy": "parallel_batch_save",
            "start_time": parallel_start_time,
            "total_sequences": len(sequences)
        }
        
        logger.info(f"Starting parallel batch embedding strategy for {len(sequences)} sequences")
        
        # embeddings_pure_batch = {}

        batch_ids = []
        batch_embeddings = []

        save_batch_processes = []
        batch_index = 0
        sequence_count = 0
        
        timing_metrics = {
            "tokenization": 0,
            "to_device": 0,
            "inference": 0,
            "post_processing_get_embeddings": 0,
            "post_processing_convert_to_numpy": 0,
            "save_batch": 0,
            "cache_clear": 0,
            "model_eval": 0
        }
        generate_embedding_total_time = 0

        sequences_count = len(sequences)
        
        try:
            t0 = time.perf_counter()
            self.model.eval()
            t1 = time.perf_counter()
            timing_metrics["model_eval"] += t1 - t0
            with torch.inference_mode():
                for prot_id, prot_seq in sequences.items():
                    sequence_count += 1
                    
                    # Generate embedding
                    # generate_embedding_start_time = time.perf_counter()
                    # embeddings_pure, timing = self.get_embedding_profiled(prot_id, prot_seq)
                    # generate_embedding_end_time = time.perf_counter()
                    # generate_embedding_total_time += generate_embedding_end_time - generate_embedding_start_time
                    
                    # timing_metrics["tokenization"] += timing["tokenization"]
                    # timing_metrics["to_device"] += timing["to_device"]
                    # timing_metrics["inference"] += timing["inference"]
                    # timing_metrics["post_processing_get_embeddings"] += timing["post_processing_get_embeddings"]
                    # timing_metrics["post_processing_convert_to_numpy"] += timing["post_processing_convert_to_numpy"]
                    
                    t0 = time.perf_counter()
                    embeddings_pure = self.get_embedding(prot_id, prot_seq)
                    t1 = time.perf_counter()
                    generate_embedding_total_time += t1 - t0

                    # Validate embedding dimensions
                    # assert len(prot_seq) == embeddings_pure.shape[0], f'Invalid character in {prot_id}'
                    
                    batch_ids.append(prot_id)
                    batch_embeddings.append(embeddings_pure)
                    
                    # Save batch when full
                    if len(batch_ids) >= self.batch_size:
                        t0 = time.perf_counter()
                        embeddings_pure_batch = dict(zip(batch_ids, batch_embeddings))
                        # Create shared memory and spawn save process
                        metadata = create_shared_memory_batch(embeddings_pure_batch)
                        p = Process(target=save_batch_profiled_from_shared_memory, 
                                  args=(parallel_output_path, batch_index, metadata))
                        p.start()
                        save_batch_processes.append((p, parallel_output_path / f"batch_{batch_index}.h5", 
                                                    batch_ids.copy()))
                        
                        batch_ids.clear()
                        batch_embeddings.clear()
                        batch_index += 1
                        t1 = time.perf_counter()
                        timing_metrics["save_batch"] += t1 - t0
                    
                    if sequence_count % 1000 == 0:
                        logger.info(f"Parallel mode: Processed {sequence_count}/{sequences_count} sequences")
                    
                    # t0 = time.perf_counter()
                    # torch.cuda.empty_cache()
                    # t1 = time.perf_counter()
                    # timing_metrics["cache_clear"] += t1 - t0
                
                # Handle remaining sequences in final batch
                if len(batch_ids) > 0:
                    t0 = time.perf_counter()
                    embeddings_pure_batch = dict(zip(batch_ids, batch_embeddings))
                    metadata = create_shared_memory_batch(embeddings_pure_batch)
                    p = Process(target=save_batch_profiled_from_shared_memory, 
                              args=(parallel_output_path, batch_index, metadata))
                    p.start()
                    save_batch_processes.append((p, parallel_output_path / f"batch_{batch_index}.h5", 
                                                batch_ids.copy()))
                    t1 = time.perf_counter()
                    timing_metrics["save_batch"] += t1 - t0
        finally:
            # Cleanup model memory
            t0 = time.perf_counter()
            if hasattr(self, 'model') and self.model is not None:
                torch.cuda.empty_cache()
            gc.collect()
            t1 = time.perf_counter()
            timing_metrics["cache_clear"] += t1 - t0
        
        # Wait for all save processes to complete
        process_join_start = time.perf_counter()
        final_index = {}
        
        for p, batch_file, prot_ids in save_batch_processes:
            p.join()
            for prot_id in prot_ids:
                final_index[prot_id] = str(batch_file)
        
        process_join_end = time.perf_counter()
        parallel_end_time = time.perf_counter()
        
        performance_metrics.update({
            "end_time": parallel_end_time,
            "total_duration": parallel_end_time - parallel_start_time,
            "sequences_per_second": len(sequences) / (parallel_end_time - parallel_start_time),
            "process_join_time": process_join_end - process_join_start,
            "generate_embedding_total_time": generate_embedding_total_time,
            "timing_metrics": timing_metrics
        })
        
        logger.info(f"Parallel batch strategy completed in {performance_metrics['total_duration']:.2f}s")
        return final_index, performance_metrics

    def _run_batch_end_embedding(self, sequences: Dict[str, str], output_path: Path) -> Tuple[Dict[str, str], Dict[str, Any]]:
        """
        Run embedding process with batch-end saving strategy.
        
        Args:
            sequences: Dictionary mapping protein IDs to sequences
            output_path: Path to save embedding files
            
        Returns:
            tuple: (final_index, performance_metrics)
        """
        batch_end_start_time = time.perf_counter()
        batch_end_output_path = output_path / "batch_end"
        batch_end_output_path.mkdir(exist_ok=True, parents=True)
        
        performance_metrics = {
            "strategy": "batch_end_save",
            "start_time": batch_end_start_time,
            "total_sequences": len(sequences)
        }
        
        logger.info(f"Starting batch-end embedding strategy for {len(sequences)} sequences")
        
        # Accumulate all embeddings
        all_embeddings = {}

        generate_embedding_total_time = 0

        timing_metrics = {
            "tokenization": 0,
            "to_device": 0,
            "inference": 0,
            "post_processing_get_embeddings": 0,
            "post_processing_convert_to_numpy": 0,
            "save_batch": 0,
            "cache_clear": 0
        }
        
        try:
            self.model.eval()
            with torch.inference_mode():
                sequence_count = 0
                
                for prot_id, prot_seq in sequences.items():
                    sequence_count += 1
                    
                    # Generate embedding
                    # generate_embedding_start_time = time.perf_counter()
                    # embeddings_pure, timing = self.get_embedding_profiled(prot_id, prot_seq)
                    # generate_embedding_end_time = time.perf_counter()
                    # generate_embedding_total_time += generate_embedding_end_time - generate_embedding_start_time
                    # timing_metrics["tokenization"] += timing["tokenization"]
                    # timing_metrics["to_device"] += timing["to_device"]
                    # timing_metrics["inference"] += timing["inference"]
                    # timing_metrics["post_processing_get_embeddings"] += timing["post_processing_get_embeddings"]
                    # timing_metrics["post_processing_convert_to_numpy"] += timing["post_processing_convert_to_numpy"]


                    t0 = time.perf_counter()
                    embeddings_pure = self.get_embedding(prot_id, prot_seq)
                    t1 = time.perf_counter()
                    generate_embedding_total_time += t1 - t0

                    # Validate embedding dimensions
                    assert len(prot_seq) == embeddings_pure.shape[0], f'Invalid character in {prot_id}'
                    
                    # Store embedding (accumulate in memory)
                    all_embeddings[prot_id] = embeddings_pure
                    
                    if sequence_count % 50 == 0:
                        logger.info(f"Batch-end mode: Processed {sequence_count}/{len(sequences)} sequences")
                    t0 = time.perf_counter()
                    torch.cuda.empty_cache()
                    t1 = time.perf_counter()
                    timing_metrics["cache_clear"] += t1 - t0
        
        finally:
            # Cleanup model memory but keep embeddings
            t0 = time.perf_counter()
            if hasattr(self, 'model') and self.model is not None:
                torch.cuda.empty_cache()
            gc.collect()
            t1 = time.perf_counter()
            timing_metrics["cache_clear"] += t1 - t0

        embedding_generation_end = time.perf_counter()
        
        # Save all embeddings at once
        logger.info(f"Saving all {len(all_embeddings)} embeddings to disk...")
        save_start_time = time.perf_counter()
        
        save_metrics = save_all_embeddings_at_end(batch_end_output_path, all_embeddings)
        
        save_end_time = time.perf_counter()
        batch_end_end_time = time.perf_counter()
        
        # Create final index
        output_file = batch_end_output_path / "all_embeddings.h5"
        final_index = {prot_id: str(output_file) for prot_id in all_embeddings.keys()}
        
        # Update performance metrics with time data only
        performance_metrics.update({
            "end_time": batch_end_end_time,
            "total_duration": batch_end_end_time - batch_end_start_time,
            "embedding_generation_time": embedding_generation_end - batch_end_start_time,
            "saving_time": save_end_time - save_start_time,
            "sequences_per_second": len(sequences) / (batch_end_end_time - batch_end_start_time),
            "save_metrics": save_metrics,
            "generate_embedding_total_time": generate_embedding_total_time,
            "timing_metrics": timing_metrics
        })
        
        logger.info(f"Batch-end strategy completed in {performance_metrics['total_duration']:.2f}s")
        return final_index, performance_metrics
    
    def embed(self, sequences: Dict[str, str], output_path: Path) -> Dict[str, str]:
        """
        Run performance comparison between parallel batch saving and batch-end saving.
        
        Args:
            sequences: Dictionary mapping protein IDs to sequences
            output_path: Path to save embedding files and performance reports
            
        Returns:
            dict: Final index mapping protein IDs to file paths (from the last run strategy)
        """

        if len(sequences) > 3000:
            sequences = dict(list(sequences.items())[:3000])
        
        # Create output directories
        output_path.mkdir(exist_ok=True, parents=True)
        performance_log_path = output_path / "performance_comparison.json"
        
        # Initialize model
        self._initialize_model()
        
        logger.info(f"Starting performance comparison for {len(sequences)} sequences")
        logger.info(f"Device: {self.device}, Batch size: {self.batch_size}, Mode: {self.comparison_mode}")
        
        comparison_results = {
            "experiment_metadata": {
                "total_sequences": len(sequences),
                "batch_size": self.batch_size,
                "comparison_mode": self.comparison_mode,
                "timestamp": time.time()
            },
            "strategies": {}
        }
        
        final_index = {}
        
        try:
            # Run parallel batch strategy
            if self.comparison_mode in ["both", "parallel_only"]:
                logger.info("=" * 60)
                logger.info("STARTING PARALLEL BATCH SAVING STRATEGY")
                logger.info("=" * 60)
                
                # Re-initialize model for fair comparison
                self._initialize_model()
                
                parallel_index, parallel_metrics = self._run_parallel_process_parall_save_batch_embedding(sequences, output_path)
                comparison_results["strategies"]["parallel_batch_save"] = parallel_metrics
                final_index = parallel_index
                
                # Clean up model to free memory
                if hasattr(self, 'model') and self.model is not None:
                    del self.model
                    self.model = None
                torch.cuda.empty_cache()
                gc.collect()
                
                logger.info(f"Parallel strategy: {parallel_metrics['total_duration']:.2f}s, {parallel_metrics['sequences_per_second']:.2f} seq/s")
            
            # Run batch-end strategy
            # if self.comparison_mode in ["both", "batch_end_only"]:
            #     logger.info("=" * 60)
            #     logger.info("STARTING BATCH-END SAVING STRATEGY")
            #     logger.info("=" * 60)
                
            #     # Re-initialize model for fair comparison
            #     self._initialize_model()
                
            #     batch_end_index, batch_end_metrics = self._run_batch_end_embedding(sequences, output_path)
            #     comparison_results["strategies"]["batch_end_save"] = batch_end_metrics
                
            #     # If we only ran batch-end, use its index
            #     if self.comparison_mode == "batch_end_only":
            #         final_index = batch_end_index
                
            #     logger.info(f"Batch-end strategy: {batch_end_metrics['total_duration']:.2f}s, {batch_end_metrics['sequences_per_second']:.2f} seq/s")
            
            # Generate comparison summary
            # if len(comparison_results["strategies"]) > 1:
            #     parallel_metrics = comparison_results["strategies"]["parallel_batch_save"]
            #     batch_end_metrics = comparison_results["strategies"]["batch_end_save"]
                
            #     comparison_results["comparison_summary"] = {
            #         "parallel_total_time": parallel_metrics["total_duration"],
            #         "batch_end_total_time": batch_end_metrics["total_duration"],
            #         "time_difference": batch_end_metrics["total_duration"] - parallel_metrics["total_duration"],
            #         "time_difference_percent": ((batch_end_metrics["total_duration"] - parallel_metrics["total_duration"]) / parallel_metrics["total_duration"]) * 100,
            #         "parallel_throughput": parallel_metrics["sequences_per_second"],
            #         "batch_end_throughput": batch_end_metrics["sequences_per_second"],
            #         "throughput_difference_percent": ((batch_end_metrics["sequences_per_second"] - parallel_metrics["sequences_per_second"]) / parallel_metrics["sequences_per_second"]) * 100,
            #         "parallel_process_join_time": parallel_metrics.get("process_join_time", 0),
            #         "batch_end_saving_time": batch_end_metrics.get("saving_time", 0),
            #         "recommendation": self._generate_recommendation(parallel_metrics, batch_end_metrics)
            #     }
                
            #     logger.info("=" * 60)
            #     logger.info("PERFORMANCE COMPARISON SUMMARY")
            #     logger.info("=" * 60)
            #     summary = comparison_results["comparison_summary"]
            #     logger.info(f"Parallel batch saving: {summary['parallel_total_time']:.2f}s ({summary['parallel_throughput']:.2f} seq/s)")
            #     logger.info(f"Batch-end saving: {summary['batch_end_total_time']:.2f}s ({summary['batch_end_throughput']:.2f} seq/s)")
            #     logger.info(f"Time difference: {summary['time_difference']:.2f}s ({summary['time_difference_percent']:.1f}%)")
            #     logger.info(f"Throughput difference: {summary['throughput_difference_percent']:.1f}%")
            #     logger.info(f"Recommendation: {summary['recommendation']}")
        
        finally:
            # Final cleanup
            if hasattr(self, 'model') and self.model is not None:
                del self.model
                self.model = None
            torch.cuda.empty_cache()
            gc.collect()
        
        # Save performance comparison results
        try:
            with open(performance_log_path, 'w') as f:
                json.dump(comparison_results, f, indent=2, default=str)
            logger.info(f"Performance comparison results saved to: {performance_log_path}")
        except Exception as e:
            logger.warning(f"Failed to save performance comparison results: {e}")
        
        return final_index
    
    def _generate_recommendation(self, parallel_metrics: Dict[str, Any], batch_end_metrics: Dict[str, Any]) -> str:
        """Generate a recommendation based on the time performance comparison."""
        parallel_time = parallel_metrics["total_duration"]
        batch_end_time = batch_end_metrics["total_duration"]
        
        time_diff_percent = ((batch_end_time - parallel_time) / parallel_time) * 100
        
        if abs(time_diff_percent) < 5:
            return "Performance is similar between both strategies. Use parallel batch saving for better memory efficiency."
        elif time_diff_percent < -10:
            return "Batch-end saving is significantly faster."
        elif time_diff_percent > 10:
            return "Parallel batch saving is significantly faster."
        else:
            return "Performance difference is moderate. Choose based on your specific requirements."
