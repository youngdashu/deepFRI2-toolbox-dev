set -e
GROUP_DIR=$1
CONDA_DIR="$GROUP_DIR/.conda"
conda config --add pkgs_dirs "$CONDA_DIR"

conda env create --prefix $ENV_PATH --file "toolbox_env_conda.yml"

conda config --set auto_activate_base false

source activate $ENV_PATH

pip install bio~=1.7.0
pip install foldcomp~=0.0.7