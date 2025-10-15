import weaviate
import cohere 
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

def connect_to_weaviate():
   
   try: 
      client = weaviate.connect_to_local()
      collection  = client.collections.get("Instructions")
      print("Connected to Weaviate and accessed 'Instructions' collection.")
      return client, collection
   except Exception as e:
        print(f"Error connecting to Weaviate: {e}")
        exit()

client , instructions_collection = connect_to_weaviate()

def retrieve_context(query_text , collection):

    response = collection.query.near_text( #converts query text to vectors and searches for the chunk in database with the most similarities
        query = query_text,
        limit = 3
    )

    context ="\n\n---\n\n".join([item.properties['content'] for item in response.objects])# combines the 3 seperate chunks into one string with --- in between
    return context

def generate_response(query_text, context):
    co = cohere.Client()

    prompt = (
    f"Based *only* on the following context, please answer the question.\n\n"
    f"Context:\n{context}\n\n"
    f"Question: {query_text}\n"
    f"Answer:"
    )
    response  = co.chat(
        model = "command-a-03-2025",
        message = prompt,
        temperature = 0.3
    )
    return response.text

def main():
    try:
        question = "UMDE yönergesinin anlatmak istediği nedir?"

        print(f"Searching for context related to : {question}")
        retrieved_context = retrieve_context(question, instructions_collection)

        print("Generating response based on retrieved context...")
        answer = generate_response(question, retrieved_context) 

        print("\n---ANSWER---")
        print(answer)
        print("\n---CONTEXT USED---")
        print(retrieved_context)

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
    # Always close the connection when you're done
    # INFERENCE
        if client:
            client.close()
            print("\n🔌 Connection closed.")

if __name__ == "__main__":
    main()

   