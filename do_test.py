
#!/usr/bin/python3
from utils import debugInfo,debugDebug,get_embeddings,open_InMemory_vector_store,open_Chroma_vector_store,similaritySearch,DEFAULT_COLLECTION,COLLECTION_ENTITIES
from hdfs_vector_store import get_vector_store_entities,get_vector_store_geometadata,get_vector_store_data
from hdfs_data import find_services
#from langchain_text_splitters import RecursiveCharacterTextSplitter
#import bs4
from langchain import hub
#from langchain.text_splitter import RecursiveCharacterTextSplitter
#from langchain_community.document_loaders import WebBaseLoader
#from langchain_community.vectorstores import Chroma
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI, OpenAIEmbeddings


#LLM_MODEL="gpt-5-2025-08-07"
LLM_MODEL="gpt-4.1"

# Function for pre filtering
from openai import OpenAI
import json

# Get the Vector Store and Retriever
def get_retriever(collection,k):
	debugInfo(f'Collection: {collection}')
	embeddings=get_embeddings()
	vector_store=open_Chroma_vector_store(embeddings,collection)
	retriever = vector_store.as_retriever(search_type="mmr",search_kwargs={"k": k})
	return retriever

# Prompt RAG
prompt_rag = hub.pull("rlm/rag-prompt")


# Prompt Template
from langchain.prompts import ChatPromptTemplate
prompt_template = ChatPromptTemplate.from_template(
    "Responda à pergunta: {query}"
)

# LLM
llm = ChatOpenAI(model_name=LLM_MODEL)
#Not accepted in case of GPT-5, temperature=0)

# Post-processing
def format_docs(docs):
    debugDebug("Format Doc:")
    n=0
    for doc in docs:
        debugDebug(doc.metadata)
        n=n+1
    debugInfo(f"Docs to add {n}")
    return "\n\n".join(doc.page_content for doc in docs)

# GeoLocal Output 
from typing import Optional
from typing_extensions import Annotated, TypedDict

#
# Only using GPT-5 there are good results
#
class GeoLocalTest1(TypedDict):
    """Local ."""

    setup: Annotated[str, ..., "In the table, for each fiwareService are available its geographical centroid and the maximum distance of its locals related to this fiwareService"]

    # Alternatively, we could have specified setup as:

    # setup: str                    # no default, no description
    # setup: Annotated[str, ...]    # no default, no description
    # setup: Annotated[str, "foo"]  # default, no description

    services: Annotated[list[str], ..., "The fiwareService in which the distance from fiwareService's centroid to the local related to the question is less than fiwareService's max_distance."]
    distance: Annotated[list[str], ..., "Distance from the fiwareService's Centroid to the location that is related to the question"]
    local:    Annotated[list[str], ..., "Local related to the question"]

#
# Good results using GPT-4
#
class GeoLocalTest2(TypedDict):
    """Local ."""

    setup: Annotated[str, ..., "Setup of the question"]
    # Alternatively, we could have specified setup as:
    locals:       Annotated[list[str], ..., "List name of distant locals related to the question. In case of unknown location, return an empty list."]
    coordinates:  Annotated[list[str], ..., "List coordinates (decimal format) of distant locals related to the question. In case of unknown location, return an empty list."]

class NgsiEntities(TypedDict):
    """Local ."""
    setup: Annotated[str, ..., "The setup of the question"]
    entities: Annotated[list[str], ..., "List of NGSI entities"]

# Question
def do_query(chain,question):
        print(f"Question: {question}")
        response=chain.invoke(question)
        print(f"Response: {response}")
        print("."*80)

# Question supported by RAG
def do_query_rag(question):
	retriever=get_retriever(DEFAULT_COLLECTION,2)
	chain_rag = (
 	   {"context": retriever | format_docs, "question": RunnablePassthrough()}
    		| prompt_rag
    		| llm
    		| StrOutputParser()
	)
	do_query(chain_rag,question)

#do_query_rag("Describe the provided data")
#do_query_rag("Quais os nomes dos modelos de dados das entidades NGSI são utilizadas nos dados?")
#do_query_rag("How you know that these measures are from Porto?")
#do_query_rag("Present all data as a table")

def do_query_entity(question):
        vector_store=get_vector_store_entities()
        retriever = vector_store.as_retriever(search_type="mmr",search_kwargs={"k": 20})
        structured_llm = llm.with_structured_output(NgsiEntities)
        chain_rag_entities = (
           {"context": retriever | format_docs, "question": RunnablePassthrough()}
           | prompt_rag
           | structured_llm
        )
        do_query(chain_rag_entities,question)

#do_query_entity("Present all ngsi entities")
#do_query_entity("Quais os nomes dos modelos de dados das entidades NGSI a consultar para responder à questão \"Qual a temperatura actual na Rua 9 de Abril no Porto, Portugal\"?")
#do_query_entity("Quais os nomes dos modelos de dados das entidades NGSI a consultar para responder à questão \"Qual a temperatura no edifício na rua X?")

##
def do_query_geo(question):
        structured_llm = llm.with_structured_output(GeoLocal)
        do_query(structured_llm,question)

#do_query_geo("Quais as coordenadas geográficas e raio de um circulo para abranger o Porto em Portugal?")


def do_metaquery(question):
        print("*"*100)
        print('Question: '+question)
        # Step 1 - Which Entities
        metaquestion=f"Quais os nomes dos modelos de dados das entidades NGSI a consultar para responder à questão \"{question}\"?"
        #print('MetaQuestion: '+metaquestion)
        vector_store=get_vector_store_entities()
        retriever = vector_store.as_retriever(search_type="mmr",search_kwargs={"k": 20})
        structured_llm = llm.with_structured_output(NgsiEntities)
        chain_rag_entities = (
           {"context": retriever | format_docs, "question": RunnablePassthrough()}
           | prompt_rag
           | structured_llm
        )
        response=chain_rag_entities.invoke(metaquestion)
        list_of_entities=response['entities']
        print(f"List Of Entities: {list_of_entities}")

        # Step 2 - Find the best services based on its geolocation
        metaquestion=f"Qual é a lista de locais e respectivas coordenadas geográficas em formato decimal relativos à questão: \"{question}\""
        #print('MetaQuestion: '+metaquestion)
        structured_llm = llm.with_structured_output(GeoLocalTest2)
        response=structured_llm.invoke(metaquestion)
        qsetup=response['setup']
        print(f"Setup: {qsetup}")
        list_of_locals=response['locals']
        list_of_coordinates=response['coordinates']
        print(f"List Of Locals: {list_of_locals}")
        print(f"List Of Coordinates: {list_of_coordinates}")

        list_of_services=find_services(list_of_entities,list_of_locals,list_of_coordinates)
        print(f"List Of Services:")
        print(json.dumps(list_of_services, sort_keys=False, indent=2))

        if (len(list_of_services)<1):
            print("There is no data available to answer the question")
            return 

        # Step 3 - Get Data
        vector_store=get_vector_store_data(list_of_services)
        #similaritySearch(vector_store,'question',10)
        retriever = vector_store.as_retriever(search_type="mmr",search_kwargs={"k":180,"fetch_k": 180})
        chain_rag_data = (
           {"context": retriever | format_docs, "question": RunnablePassthrough()}
                | prompt_rag
                | llm
                | StrOutputParser()
        )
        response=chain_rag_data.invoke(question)
        print(f"Response: {response}")

#do_metaquery('Quais são as cidades onde existe informação sobre a temperatura ambiente?')
#do_metaquery('Apresenta como uma tabela de texto a data/hora, o local, id do registo ngsi e o nível de dioxido de carbono, na cidade do Porto à data de 25 de Agosto de 2025?')
#do_metaquery('Indicar a temperatura, e a data/hora da respectiva observação, em São Pedro de Fins na Maia?')
#do_metaquery('Indicar a temperatura, e a data/hora da respectiva observação, em Matosinhos')
#do_metaquery('Indicar a temperatura, e a data/hora da respectiva observação, em Matosinhos e Vila Nova de Gaia')
do_metaquery('Indicar a temperatura, e a data/hora da respectiva observação, em Matosinhos e Vila do Conde')
#do_metaquery('Indicar a temperatura, e a data/hora da respectiva observação, em Lisboa')
#do_metaquery('Qual é o nível de dioxido de carbono, a data/hora e local exacto da respectiva observação nos seguintes locais: Matosinhos, Vila Nova de Gaia e Avenida da Boavista no Porto')


def do_geoquery_test1(question):
        metaquestion=f"Considerando a tabela, quais são os FiwareServices que tem um centroid mais perto do local relativo à questão: \"{question}\"?"
        #print('MetaQuestion: '+metaquestion)
        vector_store=get_vector_store_geometadata(['TemperatureSensor','AirQualityObserved'])
        retriever = vector_store.as_retriever(search_type="mmr",search_kwargs={"k": 20})
        structured_llm = llm.with_structured_output(GeoLocal1)
        chain_rag_geodata = (
           {"context": retriever | format_docs, "question": RunnablePassthrough()}
           | prompt_rag
           | structured_llm
        )
        response=chain_rag_geodata.invoke(metaquestion)
        qsetup=response['setup']
        print(f"Setup: {qsetup}")
        list_of_services=response['services']
        distance=response['distance']
        local=response['local']
        print(f"List Of Services: {list_of_services}")
        print(f"distance: {distance}")
        print(f"local: {local}")

##
def do_geoquery_test2(question):
        metaquestion=f"Qual é a lista de locais e respectivas coordenada geográficas em formato decimal relativos à questão: \"{question}\"?"
        #print('MetaQuestion: '+metaquestion)
        structured_llm = llm.with_structured_output(GeoLocalTest2)
        response=structured_llm.invoke(metaquestion)
        qsetup=response['setup']
        print(f"Setup: {qsetup}")
        list_of_locals=response['locals']
        list_of_coordinates=response['coordinates']
        print(f"List Of Locals: {list_of_locals}")
        print(f"List Of Coordinates: {list_of_coordinates}")

#do_geoquery_test2('Indicar a média anual de temperaturas?')
#do_geoquery_test2('Indicar a temperatura, e a data/hora da respectiva observação, em São Pedro de Fins na Maia?')
#do_geoquery_test2('Indicar a temperatura, e a data/hora da respectiva observação, na Rua de Cedofeita no Porto?')
#do_geoquery_test2('Indicar a temperatura, e a data/hora da respectiva observação, em Vila Nova de Gaia Portugal?')
#aaa('Indicar a temperatura, e a data/hora da respectiva observação, em Vila do Conde, Portugal?')
