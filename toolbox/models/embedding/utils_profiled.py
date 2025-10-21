from pathlib import Path
from typing import Dict
import torch
import h5py
import time
import os
import threading
import json
import numpy as np
from multiprocessing import shared_memory


def save_batch_profiled(output_path: Path, batch_index: int, embeddings_pure: Dict[str, torch.Tensor]):
    """
    Profiled version of save_batch function that logs detailed timing information.
    
    This function saves a batch of embeddings to an H5 file while capturing
    comprehensive performance metrics including I/O timing, tensor sizes,
    and throughput measurements.
    
    Args:
        output_path: Directory to save the batch file
        batch_index: Index of the current batch
        embeddings_pure: Dictionary mapping protein IDs to their embedding tensors
    """
    # Start timing and metadata collection
    start_time = time.perf_counter()
    start_timestamp = time.time()
    
    batch_file = output_path / f"batch_{batch_index}.h5"
    log_file = output_path / f"save_batch_profile_{batch_index}_{os.getpid()}.json"
    
    # Initialize comprehensive log entry
    log_entry = {
        "batch_index": batch_index,
        "process_id": os.getpid(),
        "thread_id": threading.get_ident(),
        "start_timestamp": start_timestamp,
        "start_perf_counter": start_time,
        "num_sequences": len(embeddings_pure),
        "batch_file": str(batch_file),
        "sequence_ids": list(embeddings_pure.keys()),
        "performance_metrics": {},
        "timing_breakdown": {}
    }
    
    try:
        # Time the file creation and writing process
        file_create_start = time.perf_counter()
        
        with h5py.File(batch_file, 'w') as f:
            file_create_end = time.perf_counter()
            
            # Track individual dataset creation times
            dataset_times = []
            dataset_write_start = time.perf_counter()
            
            for seq_id, embedding in embeddings_pure.items():
                dataset_start = time.perf_counter()
                
                # Create dataset with compression for better I/O performance tracking
                dataset = f.create_dataset(seq_id, data=embedding)
                
                dataset_end = time.perf_counter()
                
                dataset_times.append({
                    "seq_id": seq_id,
                    "duration": dataset_end - dataset_start
                })
            
            dataset_write_end = time.perf_counter()
            
            # Force flush to ensure all data is written
            f.flush()
            
        file_close_end = time.perf_counter()
        
        # Get final file statistics
        file_stats = batch_file.stat()
        final_file_size = file_stats.st_size
        
        # Calculate comprehensive timing breakdown
        log_entry["timing_breakdown"] = {
            "file_creation": file_create_end - file_create_start,
            "dataset_writing": dataset_write_end - dataset_write_start,
            "file_closing": file_close_end - dataset_write_end,
            "total_io_time": file_close_end - file_create_start
        }
        
        # Calculate performance metrics
        total_duration = file_close_end - start_time
        
        log_entry["performance_metrics"] = {
            "total_duration": total_duration,
            "sequences_per_second": len(embeddings_pure) / total_duration if total_duration > 0 else 0,
            "bytes_per_second": final_file_size / total_duration if total_duration > 0 else 0,
            "avg_dataset_time": sum(d["duration"] for d in dataset_times) / len(dataset_times) if dataset_times else 0
        }
        
        # Success metadata
        log_entry.update({
            "end_timestamp": time.time(),
            "end_perf_counter": file_close_end,
            "success": True,
            "file_size_bytes": final_file_size,
            "dataset_count": len(dataset_times),
            "dataset_times": dataset_times
        })
        
    except Exception as e:
        # Error handling with timing
        error_time = time.perf_counter()
        
        log_entry.update({
            "end_timestamp": time.time(),
            "end_perf_counter": error_time,
            "total_duration": error_time - start_time,
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "error_occurred_at": error_time - start_time
        })
        
        # Re-raise the exception after logging
        raise
    
    finally:
        # Always attempt to write the log file
        try:
            log_entry["log_write_start"] = time.perf_counter()
            
            with open(log_file, 'w') as f:
                json.dump(log_entry, f, indent=2)
            
            log_entry["log_write_duration"] = time.perf_counter() - log_entry["log_write_start"]
            
        except Exception as log_error:
            # If logging fails, print to stderr but don't fail the main operation
            print(f"Warning: Failed to write profiling log for batch {batch_index}: {log_error}")
            print(f"Batch {batch_index} performance summary:")
            print(f"  Duration: {log_entry.get('performance_metrics', {}).get('total_duration', 'unknown')} seconds")
            print(f"  Sequences: {log_entry.get('num_sequences', 'unknown')}")
            print(f"  Success: {log_entry.get('success', 'unknown')}")


# Keep the original function available for backward compatibility
def save_batch_original(output_path: Path, batch_index: int, embeddings_pure: Dict[str, torch.Tensor]):
    """Original save_batch function without profiling."""
    batch_file = output_path / f"batch_{batch_index}.h5"
    with h5py.File(batch_file, 'w') as f:
        for seq_id, embedding in embeddings_pure.items():
            f.create_dataset(seq_id, data=embedding)


def create_shared_memory_batch(embeddings_pure: Dict[str, torch.Tensor]):
    """Serialize embeddings into shared memory blocks and return metadata."""
    batch_metadata = []
    shared_blocks = []
    try:
        for prot_id, embedding in embeddings_pure.items():
            if isinstance(embedding, torch.Tensor):
                array = embedding.detach().cpu().numpy()
            else:
                array = np.asarray(embedding)

            shm = shared_memory.SharedMemory(create=True, size=array.nbytes)
            shared_blocks.append(shm)

            shm_array = np.ndarray(array.shape, dtype=array.dtype, buffer=shm.buf)
            shm_array[...] = array

            batch_metadata.append({
                "prot_id": prot_id,
                "shape": array.shape,
                "dtype": str(array.dtype),
                "shm_name": shm.name,
            })
    except Exception:
        for shm in shared_blocks:
            shm.close()
            shm.unlink()
        raise
    finally:
        for shm in shared_blocks:
            shm.close()

    return batch_metadata


def save_batch_profiled_from_shared_memory(output_path: Path, batch_index: int, batch_metadata):
    """Reconstruct embeddings from shared memory and delegate to save_batch_profiled."""
    # embeddings_pure = {}
    shared_blocks = []
    try:
        batch_ids = []
        batch_embeddings = []
        for meta in batch_metadata:
            shm = shared_memory.SharedMemory(name=meta["shm_name"])
            shared_blocks.append(shm)

            batch_ids.append(meta["prot_id"])

            array = np.ndarray(meta["shape"], dtype=np.dtype(meta["dtype"]), buffer=shm.buf)

            batch_embeddings.append(array)
        
        embeddings_pure = dict(zip(batch_ids, batch_embeddings))

        save_batch_original(output_path, batch_index, embeddings_pure)
    finally:
        for shm in shared_blocks:
            shm.close()
            shm.unlink()
