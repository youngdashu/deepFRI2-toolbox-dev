import os
import dotenv

def get_pdir():
    dotenv.load_dotenv()
    return os.getenv("DATA_PATH")

EMBEDDINGS_PATH = lambda: get_pdir() + "/embeddings"
DISTOGRAMS_PATH = lambda: get_pdir() + "/distograms"
SEQUENCES_PATH = lambda: get_pdir() + "/sequences"
