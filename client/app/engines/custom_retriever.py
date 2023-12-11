import requests
from langchain.schema.retriever import BaseRetriever
from langchain.docstore.document import Document
from typing import List
from langchain.callbacks.manager import CallbackManagerForRetrieverRun
from langchain.text_splitter import CharacterTextSplitter


class CustomRetriever(BaseRetriever):
    """
    Retriever class to parse the documents from the DB.
    """

    index = 'demo-graz'
    lang = 'en'
    limit = 200

    def _get_relevant_documents(self, query, *, run_manager=CallbackManagerForRetrieverRun) -> List[Document]:
        params = {
            'index': self.index,
            'lang': self.lang,
            'limit': self.limit,
            'q': None
        }
        url = "http://localhost:8000/search"
        params['q'] = query

        try:
            if query is not None:
                response = requests.get(url, params=params)
                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Parse the JSON response
                    results = response.json()['results']
                    # Do something with the results
                    if results:
                        doc_creator = CharacterTextSplitter(
                            separator="\n",
                            chunk_size=1000,
                            chunk_overlap=200,
                            length_function=len
                        )
                        text_documents = [doc_['textSnippet'] for doc_ in results]
                        metadatas = []
                        for doc_ in results:
                            metadata = {k: doc_[k] for k in doc_ if k != 'textSnippet'}
                            metadata['source'] = metadata['url']
                            metadatas.append(metadata)

                        document = doc_creator.create_documents(texts=text_documents, metadatas=metadatas)
                        return document
                else:
                    print(f"Error: {response.status_code}")
        except:
            print(f"Unknown Error")
