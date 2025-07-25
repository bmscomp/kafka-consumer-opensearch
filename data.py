import pandas as pd
import json
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import numpy as np

# OpenSearch configuration
client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_compress=True,
    use_ssl=False,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False,
)


def create_index(index_name, mapping=None):
    """Create OpenSearch index with optional mapping"""
    if client.indices.exists(index=index_name):
        print(f"Index '{index_name}' already exists")
        return

    index_body = {}
    if mapping:
        index_body["mappings"] = mapping

    client.indices.create(index=index_name, body=index_body)
    print(f"Created index: {index_name}")


def load_csv_data(file_path, index_name, chunk_size=1000):
    """Load CSV data into OpenSearch"""

    # Read CSV file
    df = pd.read_csv(file_path)

    # Handle NaN values
    df = df.fillna('')

    # Convert DataFrame to documents
    def generate_docs():
        for idx, row in df.iterrows():
            doc = {
                '_index': index_name,
                '_id': idx,
                '_source': row.to_dict()
            }
            yield doc

    # Bulk insert
    try:
        success, failed = bulk(client, generate_docs(), chunk_size=chunk_size)
        print(f"Successfully indexed {success} documents")
        if failed:
            print(f"Failed to index {len(failed)} documents")
    except Exception as e:
        print(f"Error during bulk insert: {e}")


def load_json_data(file_path, index_name, chunk_size=1000):
    """Load JSON data into OpenSearch"""

    with open(file_path, 'r') as f:
        data = json.load(f)

    # Handle different JSON structures
    if isinstance(data, list):
        documents = data
    elif isinstance(data, dict) and 'data' in data:
        documents = data['data']
    else:
        documents = [data]

    def generate_docs():
        for idx, doc in enumerate(documents):
            yield {
                '_index': index_name,
                '_id': idx,
                '_source': doc
            }

    try:
        success, failed = bulk(client, generate_docs(), chunk_size=chunk_size)
        print(f"Successfully indexed {success} documents")
    except Exception as e:
        print(f"Error during bulk insert: {e}")


# Example usage
if __name__ == "__main__":
    # Example 1: Load CSV data
    csv_file = "./data/your-dataset.csv"
    index_name = "kaggle-dataset"

    create_index(index_name)
    load_csv_data(csv_file, index_name)

    # Example 2: Load JSON data
    # json_file = "./data/your-dataset.json"
    # load_json_data(json_file, "kaggle-json-dataset")
