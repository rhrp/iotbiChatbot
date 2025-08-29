#!/usr/bin/python3

# Opcao WebPages
#import bs4
#from langchain_community.document_loaders import WebBaseLoader
# Load and chunk contents of the blog
#loader = WebBaseLoader(
#    web_paths=("https://lilianweng.github.io/posts/2023-06-23-agent/",),
#    bs_kwargs=dict(
#        parse_only=bs4.SoupStrainer(
#            class_=("post-content", "post-title", "post-header")
#        )
#    ),
#)
#docs = loader.load()

DEFAULT_COLLECTION='test_collection'
COLLECTION_ENTITIES='ngsiEntities'

# Debug
LEVEL_ERROR=3
LEVEL_INFO=2
LEVEL_DEBUG=1
LEVEL_NONE=99

DEBUG_LEVEL=LEVEL_INFO
DEBUG_LEVELS={}
DEBUG_LEVELS[LEVEL_ERROR]="ERROR"
DEBUG_LEVELS[LEVEL_DEBUG]="DEBUG"
DEBUG_LEVELS[LEVEL_INFO]="INFO"

def debug(p_msg,p_level):
	if p_level>=DEBUG_LEVEL:
		td=DEBUG_LEVELS[p_level]
		from time import gmtime, strftime
		tm=strftime("%Y-%m-%d %H:%M:%S", gmtime())
		print(f"{tm} :: {__name__} :: {td} :: {p_msg}");
def debugError(p_msg):
	debug(p_msg,LEVEL_ERROR)
def debugDebug(p_msg):
	debug(p_msg,LEVEL_DEBUG)
def debugInfo(p_msg):
	debug(p_msg,LEVEL_INFO)

# Get the embeddings object
def get_embeddings():
	from langchain_openai import OpenAIEmbeddings
	embeddings = OpenAIEmbeddings(model="text-embedding-3-large")
	return embeddings

# Open the memory-based VectorStore 
def open_InMemory_vector_store(embeddings):
	from langchain_core.vectorstores import InMemoryVectorStore
	vector_store = InMemoryVectorStore(embeddings)
	return vector_store;

# Open the Chroma-based VectorStore
def open_Chroma_vector_store(embeddings,collection_name):
	from langchain_chroma import Chroma
	vector_store = Chroma(
	collection_name=collection_name,
		embedding_function=embeddings,
		persist_directory="./chroma_langchain_db",  # Where to save data locally, remove if not necessary
	)
	return vector_store

#Create a InMomory Vector Store
def saveInMemory(url_csv,p_vector_store):
        import os
        if os.path.exists(url_csv):
                debugDebug(f"The file {url_csv} exists.")
        else:
                debugError("The file %s does not exist." % url_csv)
                return None
        from langchain_community.document_loaders.csv_loader import CSVLoader
        loader = CSVLoader(file_path=url_csv)
        docs = loader.load()
        #
        #print(docs);

        from langchain_text_splitters import RecursiveCharacterTextSplitter
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
        all_splits = text_splitter.split_documents(docs)

        if p_vector_store==None:
                debugInfo('Init Vector Store')
                embeddings=get_embeddings();
                vector_store=open_InMemory_vector_store(embeddings)
        else:
                vector_store=p_vector_store
        # Index chunks
        ret = vector_store.add_documents(documents=all_splits)
        #debugInfo(ret)
        #sz=vector_store.get(include=["embeddings"])
        #debugInfo(f'VectoStore size: {sz}')
        return vector_store

def displayDocs(docs):
        i=0;
        for doc in docs:
                print(f"\nDocumento[{i}]::{doc.id}: {doc}")
                i=i+1

# Perform a similarity search
def similaritySearch(vector_store,search,k):
        print(f"Search Vector Store: \"{search}\"")
        docs=vector_store.similarity_search(search,k=k)
        displayDocs(docs)

# Find common values in two arrays
def find_common_elements(arr1, arr2):
    # Convert arrays to sets for faster lookup
    set1 = set(arr1)
    set2 = set(arr2)
    # Find intersection of the sets (common elements)
    common_elements = set1.intersection(set2)
    return list(common_elements)
