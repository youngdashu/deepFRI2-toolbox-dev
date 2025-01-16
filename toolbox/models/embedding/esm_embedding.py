from typing import Dict
import torch
import tqdm

import h5py
import os

from itertools import islice
from transformers import AutoTokenizer, AutoModelForMaskedLM


# Parameters
esm_batch_size = 1000
device     = 'cpu'  # cpu
model_name = 'esm2_t33_650M_UR50D'

# Model
esm_tokenizer = AutoTokenizer.from_pretrained(f"facebook/{model_name}")
esm_model = AutoModelForMaskedLM.from_pretrained(f"facebook/{model_name}").to(device).to(dtype=torch.bfloat16)


# TODO: save `embeddings_pure` to files in batches


def embed(sequences: Dict[str, str], output_path: str):

    esm_model.eval()
    with torch.inference_mode():
        # Process in batches
        for i in tqdm.tqdm(range(0, len(sequences), esm_batch_size)):
            batch_file = f"{output_path}_batch_{i}.h5"
            
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

            # Save batch embeddings to h5 file
            with h5py.File(batch_file, 'w') as f:
                for seq_id, embedding in embeddings_pure.items():
                    f.create_dataset(seq_id, data=embedding)


