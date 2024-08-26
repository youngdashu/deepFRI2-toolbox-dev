import sys
import os
import dotenv
import pathlib

dotenv.load_dotenv()
data_path = os.getenv("DATA_PATH")
data_path = pathlib.Path(data_path).parent / "deepFRI2-toolbox-dev"

sys.path.append(str(data_path))
