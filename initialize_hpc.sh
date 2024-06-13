GROUP_DIR="$PLG_GROUPS_STORAGE/plggdeepfri2"

set -x

if [[ ! -d "deepfri" ]]
then
  mkdir deepfri
fi

cd deepfri

DATA_DIR="deepfri/dev_data"
mkdir "$GROUP_DIR/$DATA_DIR"

module load python
module load miniconda3

ENV_PATH="$GROUP_DIR/deepfri/dev_env"

if [[ -d "deepFRI2-toolbox-dev" ]]
then
  cd "deepFRI2-toolbox-dev"
  git pull

  source activate $ENV_PATH

else
  git clone https://github.com/youngdashu/deepFRI2-toolbox-dev.git
  cd "deepFRI2-toolbox-dev"
  echo "DATA_PATH=$PLG_GROUPS_STORAGE/plggdeepfri2/$DATA_DIR" > .env
  echo "SEPARATOR=-" >> .env

  CONDA_DIR="$GROUP_DIR/.conda"
  mkdir -p "$CONDA_DIR"
  conda config --add pkgs_dirs "$CONDA_DIR"

  conda env create --prefix $ENV_PATH --file "dev_env_conda.yml"

  conda config --set auto_activate_base false

  source activate $ENV_PATH

  pip3 install foldcomp

fi







