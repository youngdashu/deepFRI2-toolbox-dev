from typing import Dict  
from pathlib import Path  
import torch  
import tqdm  
import h5py  
import gc  # Added for garbage collection  
from transformers import AutoTokenizer, AutoModelForMaskedLM  
from multiprocessing import Process  


# Parameters  
device = 'cuda'  
dtype = torch.float32  
model_name = 'esm2_t33_650M_UR50D'  
batch_size = 1000  


def save_batch(output_path: Path, batch_index: int, embeddings_pure: Dict[str, torch.Tensor]):  
    """Function to save a batch of embeddings to an H5 file."""  
    batch_file = output_path / f"batch_{batch_index}.h5"  
    with h5py.File(batch_file, 'w') as f:  
        for seq_id, embedding in embeddings_pure.items():  
            f.create_dataset(seq_id, data=embedding)  


def embed(  
    sequences: Dict[str, str],  
    output_path: Path,  
    embedding_batch_size: int=batch_size,  
):  
    save_batch_processes = []  

    # Tokenizer and model  
    esm_tokenizer = AutoTokenizer.from_pretrained(f"facebook/{model_name}")  
    esm_model = AutoModelForMaskedLM.from_pretrained(f"facebook/{model_name}").to(device).to(dtype=dtype)  

    try:  
        esm_model.eval()  
        with torch.inference_mode():  
            embeddings_pure_batch = {}  
            batch_index = 0  
            for prot_id, prot_seq in tqdm.tqdm(sequences.items()):  
                # Tokenize sequence batch  
                inputs = esm_tokenizer(prot_seq, return_tensors="pt")  
                inputs = {k: v.to(device) for k, v in inputs.items()}  

                # Get embeddings  
                outputs = esm_model(**inputs, output_hidden_states=True)  
                embeddings = outputs.hidden_states[-1]  
                # Remove START / END positions and convert to numpy arrays  
                embeddings_pure = embeddings[0,1:-1].to('cpu').detach().to(torch.float32).numpy()  
                assert len(prot_seq) == embeddings_pure.shape[0], f'Invalid character in {prot_id}'  

                embeddings_pure_batch.update({prot_id: embeddings_pure})  

                # Clear GPU memory after each iteration  
                del outputs, embeddings, inputs  
                torch.cuda.empty_cache()  

                if len(embeddings_pure_batch) >= embedding_batch_size:  
                    p = Process(target=save_batch, args=(output_path, batch_index, embeddings_pure_batch.copy()))  
                    p.start()  
                    save_batch_processes.append((p, output_path / f"batch_{batch_index}.h5", list(embeddings_pure_batch.keys())))  
                    embeddings_pure_batch.clear()  
                    batch_index += 1  

            # Save any remaining embeddings  
            if embeddings_pure_batch:  
                p = Process(target=save_batch, args=(output_path, batch_index, embeddings_pure_batch.copy()))  
                p.start()  
                save_batch_processes.append((p, output_path / f"batch_{batch_index}.h5", list(embeddings_pure_batch.keys())))  

    finally:  
        # Cleanup model and tokenizer  
        del esm_model  
        del esm_tokenizer  
        torch.cuda.empty_cache()  
        gc.collect()  

    final_index = {}  
    for p, file_path, ids in save_batch_processes:  
        p.join()  
        for prot_id in ids:  
            final_index[prot_id] = str(file_path)

    return final_index
