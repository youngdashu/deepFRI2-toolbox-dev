from models.manage_dataset.database_type import DatabaseType
from models.manage_dataset.structures_dataset import CollectionType, StructuresDataset


if __name__ == '__main__':
    StructuresDataset(
            db_type=DatabaseType.AFDB.name,
            collection_type=CollectionType.part.name,
            type_str="e_coli"
        ).create_dataset()
