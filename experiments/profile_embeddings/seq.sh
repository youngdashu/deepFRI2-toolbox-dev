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

DATASET_PATH=${MAIN_PATH}/toolbox/data/datasets/AFDB-subset--TEST_10K

# Create profiling output directory
PROFILE_OUTPUT_DIR=${DEEPFRI_PATH}/experiments/profile_embeddings/profile_logs
mkdir -p ${PROFILE_OUTPUT_DIR}

echo "Running embedding with profiling enabled..."
echo "Profile logs will be saved to: ${PROFILE_OUTPUT_DIR}"

PYTHONPATH='.' python3 -u ${DEEPFRI_PATH}/toolbox.py \
 generate_sequence \
 -p ${DATASET_PATH} 

echo "Embedding completed. Check for profile logs:"
ls -la ${PROFILE_OUTPUT_DIR}/
echo "You can analyze the results with:"
echo "python toolbox/utils/profile_analyzer.py --log-dir ${PROFILE_OUTPUT_DIR}"
