import os
import dotenv

def get_pdir():
    dotenv.load_dotenv()
    return os.getenv("DATA_PATH")

repo_path = get_pdir() + "/structures"
datasets_path = get_pdir() + "/datasets"
EMBEDDINGS_PATH = get_pdir() + "/embeddings"
DISTOGRAMS_PATH = get_pdir() + "/distograms"
SEQUENCES_PATH = get_pdir() + "/sequences"
