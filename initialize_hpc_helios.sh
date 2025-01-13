GROUP_DIR="$PLG_GROUPS_STORAGE/plggsano/tomaszlab"

set -x
set -e

if [[ ! -d "deepfri" ]]
then
  mkdir deepfri
fi

cd deepfri

DATA_DIR="deepfri/dev_data"
mkdir "$GROUP_DIR/$DATA_DIR"

module load GCCcore
module load git
module load Miniconda3
eval "$(conda shell.bash hook)"


ENV_PATH="$GROUP_DIR/deepfri/dev_env"

if [[ -d "deepFRI2-toolbox-dev" ]]
then
  cd "deepFRI2-toolbox-dev"
  git pull

  conda activate $ENV_PATH

else
  git clone https://github.com/youngdashu/deepFRI2-toolbox-dev.git
  cd "deepFRI2-toolbox-dev"
  echo "DATA_PATH=$GROUP_DIR/$DATA_DIR" > .env
  echo "SEPARATOR=-" >> .env

  CONDA_DIR="$GROUP_DIR/.conda"
  mkdir -p "$CONDA_DIR"
  conda config --add pkgs_dirs "$CONDA_DIR"

  conda env create --prefix $ENV_PATH --file "dev_env_conda.yml"

  conda config --set auto_activate_base false

  conda activate $ENV_PATH

  pip install bio~=1.7.0
  pip install foldcomp~=0.0.7

  # cd $GROUP_DIR/deepfri

  # conda create -n embedding_env python=3.9 pytorch torchvision torchaudio pytorch-cuda=12.4 -c pytorch -c nvidia

fi

PYTHONPATH='.' python3 -u ./toolbox/initialize_repository.py
