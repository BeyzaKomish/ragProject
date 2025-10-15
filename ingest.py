import weaviate # to talk to weaviate database
import weaviate.classes as wsc
import fitz # to read text pdf files
import os # to read files from directory

def connect_to_weaviate():
    """Establishes a connection to the local Weaviate instance."""
    try:
        # Weaviate's Python client is smart enough to find the local instance
        # running on http://localhost:8080 by default.
        client = weaviate.connect_to_local()
        print("✅ Successfully connected to Weaviate!")
        return client
    except Exception as e:
        print(f"❌ An error occurred connecting to Weaviate: {e}")
        # If we can't connect, we stop the script.
        exit()


def load_files_from_directory(directory):
    files = []
    print(f"Loading files from directory: {directory}")
    for filename in os.listdir(directory):
        if filename.endswith(".pdf"):
            file_path = os.path.join(directory, filename)
            doc = fitz.open(file_path)
        

            text = ""
            for page in doc:
                text += page.get_text("text")

            files.append({"filename": filename, "text": text})
            print(f"Loaded file: {filename}")
    return files

def chunk_test(files):
    chunked_data = []
    print("Chunking files...")
    for file in files:
        chunks = file["text"].split("\n\n") # simple chunking by paragraphs
        for i, chunk_test in enumerate(chunks):
            chunked_data.append({
                "content": chunk_test.strip(),
                "source": file["filename"],
            })
        print(f"Chunked {file['filename']} into {len(chunks)} chunks.")
        return chunked_data
        

# main ingestion logic 
def main():

    client = connect_to_weaviate()
    data_folder = "data" # folder containing pdf files
    collection_name = "Instructions" # weaviate collection name

    #preparing the collection in weaviate
    # deleting the collection if it already exists
    if client.collections.exists(collection_name):
        client.collections.delete(collection_name)
        print(f"Deleted existing collection: {collection_name}")

    # creating the collection
    instrunctions_collection = client.collections.create(
        name = collection_name,
        vectorizer_config = weaviate.classes.config.Configure.Vectorizer.text2vec_cohere()
    )
    print(f"Created collection: {collection_name}")

    # load and chunk the documents
    raw_files = load_files_from_directory(data_folder)
    data_to_ingest = chunk_test(raw_files)

    # ingest the chunked data into weaviate
    print("Ingesting data into Weaviate...")
    with instrunctions_collection.batch.dynamic() as batch:
        for record in data_to_ingest:
            batch.add_object(properties = record)

    print("Data ingestion complete.")

    count = instrunctions_collection.aggregate.over_all(total_count = True)
    print(f"Total records in collection '{collection_name}': {count.total_count}")

    client.close()
    print("Weaviate client connection closed.")

if __name__ == "__main__":
    main()

   