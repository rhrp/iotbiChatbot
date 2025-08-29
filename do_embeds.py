#! /usr/bin/python3
from utils import get_embeddings,open_InMemory_vector_store,open_Chroma_vector_store,DEFAULT_COLLECTION,COLLECTION_ENTITIES
import os
import optparse

g_verbose=False
g_collection=DEFAULT_COLLECTION

def main():
	parser = optparse.OptionParser()
	parser.add_option('-f','--filename',dest='filename')
	parser.add_option('-l','--load',dest='loadEntities',action='store_true')
	parser.add_option('-p','--similaritySearch',dest='similarity_search')
	parser.add_option('-s','--show',dest='show', action='store_true')
	parser.add_option('-d','--showDocs',dest='showDocs', action='store_true')
	parser.add_option('-e','--showVectors',dest='showVects', action='store_true')
	parser.add_option('-c','--collection',dest='collection')
	parser.add_option('-v', dest='verbose', action='store_true')
	opts, args = parser.parse_args()
	#process(args, output=opts.output, verbose=opts.verbose)
	if len(args) != 0:
		parser.error(f"incorrect number of arguments={len(args)}")
	if opts.verbose:
		print("Verbose mode")
		g_verbose=True
	if opts.collection!=None:
		g_collection=opts.collection
		print('Using collection: ',g_collection)
	if opts.show:
		showVectorStore(g_collection,None)
	elif opts.showDocs:
		showVectorStore(g_collection,'documents')
	elif opts.showVects:
                showVectorStore(g_collection,'embeddings')
	elif opts.filename!=None:
		save(opts.filename,g_collection)
	elif opts.loadEntities:
		load_collection_entities()
	elif opts.similarity_search!=None:
		similaritySearch(g_collection,opts.similarity_search)
	else:
		print('Args: ',args)
		print('Ops: ',opts)
		#parser.print_usage()
		parser.print_help()
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


# Add a document to the VectorStore
def save(url_csv,collection_name):
	if os.path.exists(url_csv):
		if g_verbose:
			print("The file exists.")
	else:
		print("The file %s does not exist." % url_csv)
		return
	from langchain_community.document_loaders.csv_loader import CSVLoader
	loader = CSVLoader(file_path=url_csv)
	docs = loader.load()
	#
	#print(docs);

	from langchain_text_splitters import RecursiveCharacterTextSplitter
	text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
	all_splits = text_splitter.split_documents(docs)

	embeddings=get_embeddings();

	vector_store=open_Chroma_vector_store(embeddings,collection_name);
	# Index chunks
	ret = vector_store.add_documents(documents=all_splits)
	print(ret)

# Prints the content (documents or embeddings) of a collection in the VectorStore
def showVectorStore(collection_name,prop):
	embeddings=get_embeddings();
	vector_store=open_Chroma_vector_store(embeddings,collection_name);
	print("Show Vector Store")
	# Present
	#print(vector_store.get_by_ids(['974f2f00-2b1d-455d-beaf-b2d47bdb34f6']))
	#print(vector_store.get())
	if prop==None:
		for key in vector_store.get():
			obj=vector_store.get().get(key)
			print("="*100)
			print(key)
			print("-"*100)
			if key=='ids':
				for id in obj:
					print(vector_store.get_by_ids([id]))
			elif type(obj) is list:
				for val in obj:
					print(f"\t{val}")
			elif type(obj) is tuple:
				print(key,'  is a tuple')
			else:
				print(key,' ',type(obj))
			print('')
	elif prop=='embeddings':
		results = vector_store.get(include=["embeddings"])
		# Os vectores estão em results['embeddings']
		for i, vector in enumerate(results['embeddings']):
			print(f"Vector {i}: {vector}")
	elif prop=='documents':
		i=0;
		for id in vector_store.get().get('ids'):
			print(f"\nDocumento[{i}]::{id}: {vector_store.get_by_ids([id])}")
			i=i+1
	else:
		print('Indique outra prop')

def displayDocs(docs):
	i=0;
	for doc in docs:
		print(f"\nDocumento[{i}]::{doc.id}: {doc}")
		i=i+1

# Perform a similarity search
def similaritySearch(collection_name,search):
	embeddings=get_embeddings();
	vector_store=open_Chroma_vector_store(embeddings,collection_name);
	print(f"Search Vector Store: \"{search}\"")
	docs=vector_store.similarity_search(search,k=1)
	displayDocs(docs)

def load_collection_entities():
	tmp_file="/tmp/aaaa.csv"
	from hdfs import InsecureClient
	client = InsecureClient('http://api.iotbi.tech:5000', user='hadoop')
	fnames = client.list('/system/metadata/')
	print(fnames)
	client.download('/system/metadata/entities.csv',tmp_file, n_threads=5)
	save(tmp_file,COLLECTION_ENTITIES)
if __name__ == '__main__':
	main();
