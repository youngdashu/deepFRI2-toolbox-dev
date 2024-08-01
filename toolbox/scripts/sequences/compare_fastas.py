from collections import defaultdict

from Bio import SeqIO
import dask.bag as db


def read_fasta_as_generator(file_path):
    return SeqIO.parse(file_path, "fasta")


def process_record(record, smaller_set):
    if record.id in smaller_set:
        return (record.id, str(record.seq))
    return None


def compare_fastas(smaller_fasta, bigger_fasta):
    # Read the smaller FASTA file and create a set of its protein IDs
    smaller_set = set(record.id for record in read_fasta_as_generator(smaller_fasta))

    # Create a Dask bag from the bigger FASTA file
    bigger_bag = db.from_sequence(read_fasta_as_generator(bigger_fasta), npartitions=100)

    # Process the bigger FASTA file and keep only the records present in the smaller set
    filtered_bag = bigger_bag.map(lambda record: process_record(record, smaller_set)).filter(lambda x: x is not None)

    # Collect the results
    bigger_dict = dict(filtered_bag.compute())

    # Now process the smaller FASTA file and compare with the filtered bigger dict
    differences = []
    stats = defaultdict(int)

    for record in read_fasta_as_generator(smaller_fasta):
        protein_id = record.id
        smaller_seq = str(record.seq)

        if protein_id in bigger_dict:
            bigger_seq = bigger_dict[protein_id]
            if smaller_seq == bigger_seq:
                stats['same'] += 1
            else:
                stats['different'] += 1
                differences.append((protein_id, smaller_seq, bigger_seq))
        else:
            stats['missing'] += 1
            differences.append((protein_id, smaller_seq, "Not found in bigger FASTA"))

    # Print statistics
    total_proteins = len(smaller_set)

    # Print differences
    print("\nDifferences found:")
    for protein_id, seq1, seq2 in differences:
        print(f"Protein: {protein_id}")
        print(f"Smaller FASTA sequence: {seq1}")
        print(f"Bigger FASTA sequence: {seq2}")
        print("---")

    print(f"Total proteins in smaller FASTA: {total_proteins}")
    print(f"Identical proteins: {stats['same']}")
    print(f"Different proteins: {stats['different']}")
    print(f"Missing proteins: {stats['missing']}")
    print(f"Percentage of differences: {(stats['different'] + stats['missing']) / total_proteins * 100:.2f}%")


if __name__ == "__main__":

    smaller_fasta = "/Users/youngdashu/sano/deepFRI2-toolbox-dev/data/datasets/PDB-subset--20240731_1535/sequences.fasta"
    bigger_fasta = "/Users/youngdashu/downloads/pdb_seqres.txt"

    compare_fastas(smaller_fasta, bigger_fasta)