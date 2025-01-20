from typing import Dict
import torch
import tqdm

import h5py
import os

from itertools import islice
from transformers import AutoTokenizer, AutoModelForMaskedLM
from multiprocessing import Process  # Added import for parallel processing


# Parameters
esm_batch_size = 10
device     = 'cuda'  # cpu
model_name = 'esm2_t33_650M_UR50D'

# Model
esm_tokenizer = AutoTokenizer.from_pretrained(f"facebook/{model_name}")
esm_model = AutoModelForMaskedLM.from_pretrained(f"facebook/{model_name}").to(device).to(dtype=torch.bfloat16)


def save_batch(output_path: str, batch_index: int, embeddings_pure: Dict[str, torch.Tensor]):
    """Function to save a batch of embeddings to an H5 file."""
    batch_file = f"{output_path}_batch_{batch_index}.h5"
    with h5py.File(batch_file, 'w') as f:
        for seq_id, embedding in embeddings_pure.items():
            f.create_dataset(seq_id, data=embedding)


def embed(sequences: Dict[str, str], output_path: str, embedding_batch_size: int = 1000):

    esm_model.eval()
    with torch.inference_mode():
        embeddings_pure_batch = {}
        batch_index = 0
        for i in tqdm.tqdm(range(0, len(sequences), esm_batch_size)):
            # Get batch
            batch_ids, batch_seqs = zip(*islice(sequences.items(), i, i + esm_batch_size))

            # Tokenize sequence batch
            inputs = esm_tokenizer(batch_seqs, return_tensors="pt", padding=True)
            inputs = {k: v.to(device) for k, v in inputs.items()}

            # Get embeddings
            outputs = esm_model(**inputs, output_hidden_states=True)
            embeddings = outputs.hidden_states[-1]

            # Store in dict
            embeddings_pure = {}
            for j, seq_id in enumerate(batch_ids):
                mask = inputs['attention_mask'][j]
                non_zero = mask.nonzero(as_tuple=True)[0]
                # Remove padding
                # Remove START / END positions
                # Convert to numpy arrays
                embeddings_pure[seq_id] = embeddings[j, non_zero, :][1:-1].to('cpu').detach().to(torch.float32).numpy()
                assert len(sequences[seq_id]) == embeddings_pure[seq_id].shape[0], f'Invalid character in {seq_id}'

            embeddings_pure_batch.update(embeddings_pure)

            if len(embeddings_pure_batch) >= embedding_batch_size:
                # Start a new process to save the current batch
                p = Process(target=save_batch, args=(output_path, batch_index, embeddings_pure_batch.copy()))
                p.start()
                embeddings_pure_batch.clear()
                batch_index += 1

        # Save any remaining embeddings
        if embeddings_pure_batch:
            p = Process(target=save_batch, args=(output_path, batch_index, embeddings_pure_batch.copy()))
            p.start()


