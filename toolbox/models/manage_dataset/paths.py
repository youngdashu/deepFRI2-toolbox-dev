import os

import dotenv

dotenv.load_dotenv()

pdir = os.getenv("DATA_PATH")
repo_path = pdir + "/repo"
datasets_path = pdir + "/datasets"
EMBEDDINGS_PATH = pdir + "/embeddings"
DISTOGRAMS_PATH = pdir + "/distograms"
