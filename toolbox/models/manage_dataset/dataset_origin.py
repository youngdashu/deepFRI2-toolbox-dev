import asyncio
import os

from foldcomp.setup import download
import dotenv
dotenv.load_dotenv()

pdir = os.getenv('DATA_PATH')
repo_path = pdir + "/repo"
datasets_path = pdir + "/datasets"
embeddings_path = pdir + "/embeddings"


def foldcomp_download(db: str, output_dir: str):
    print(f"Foldcomp downloading db: {db} to {output_dir}")
    download_chunks = 16
    for i in ["", ".index", ".dbtype", ".lookup", ".source"]:
        asyncio.run(
            download(
                f"https://foldcomp.steineggerlab.workers.dev/{db}{i}",
                f"{output_dir}/{db}{i}",
                chunks=download_chunks,
            )
        )
