#! /usr/bin/python3
from utils import get_embeddings,open_InMemory_vector_store,open_Chroma_vector_store,DEFAULT_COLLECTION
#from langchain_text_splitters import RecursiveCharacterTextSplitter
#import bs4
from langchain import hub
#from langchain.text_splitter import RecursiveCharacterTextSplitter
#from langchain_community.document_loaders import WebBaseLoader
#from langchain_community.vectorstores import Chroma
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

# Function for pre filtering
from openai import OpenAI
import json

# Get the Vector Store and Retriever
embeddings=get_embeddings()
vector_store=open_Chroma_vector_store(embeddings,DEFAULT_COLLECTION)
retriever = vector_store.as_retriever(search_type="mmr",search_kwargs={"k": 2})

# Prompt
prompt = hub.pull("rlm/rag-prompt")

# LLM
llm = ChatOpenAI(model_name="gpt-3.5-turbo", temperature=0)

# Post-processing
def format_docs(docs):
    print("Format Doc:")
    for doc in docs:
        print(doc.metadata);
    return "\n\n".join(doc.page_content for doc in docs)

# Chain
rag_chain = (
    {"context": retriever | format_docs, "question": RunnablePassthrough()}
    | prompt
    | llm
    | StrOutputParser()
)

# Question
def do_query(question):
	print(f"Question: {question}")
	response=rag_chain.invoke(question)
	print(response)
	print("."*80)

do_query("Describe the provided data")
do_query("Quais os nomes dos modelos de dados das entidades NGSI são utilizadas nos dados?")
do_query("How you know that these measures are from Porto?")
do_query("Present all data as a table")
