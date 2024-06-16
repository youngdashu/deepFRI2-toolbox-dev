from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.structures_dataset import StructuresDataset, CollectionType


if __name__ == '__main__':
    StructuresDataset(
        db_type=DatabaseType.AFDB.name,
        collection_type=CollectionType.part.name,
        type_str="afdb_swissprot_v4"
    ).create_dataset()

