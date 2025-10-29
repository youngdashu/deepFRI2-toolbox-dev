eval "$(mamba shell hook --shell bash)"
MAIN_PATH=/mnt/vdb2/var/storage/deepfri2
DEEPFRI_PATH=${MAIN_PATH}/toolbox/deepFRI2-toolbox-dev

# DATE=20241105
# DATE=20250114
DATE=20250908

IDS_PATH=${MAIN_PATH}/toolbox/scripts/input_generation/uniprot/test_10k.txt
# IDS_PATH=${MAIN_PATH}/data/inputs/${DATE}/uids_uniprot_with_prefixes.txt

AFDB_PATH=${MAIN_PATH}/data/afdb/structures

cd ${DEEPFRI_PATH}
mamba activate ${MAIN_PATH}/toolbox/tbe

EMBEDDER_TYPE=esmc_600m
# EMBEDDER_TYPE=esm2_t33_650M_UR50D

PYTHONPATH='.' python3 -u ${DEEPFRI_PATH}/toolbox.py \
 dataset \
 -d AFDB \
 -c subset \
 --version TEST_10K  \
 -i ${IDS_PATH} \
 --input-path ${AFDB_PATH} \
 --overwrite
 # -e ${EMBEDDER_TYPE} #\
# -v