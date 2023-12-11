import os
from app.engines.custom_retriever import CustomRetriever
from langchain.memory import ConversationBufferWindowMemory
from langchain.chains import RetrievalQA
from dotenv import load_dotenv
from langchain.chat_models import ChatOpenAI
from app.config import OPENAI_API_KEY

class QaChainManager:
    def __init__(self, model='gpt-4', max_tokens=256, n_conversations_memory=5,
                 index='demo-graz', lang='en', limit=100):
        """
        Initializes a QA chain using the specified parameters
        """
        # Load env variables (OPENAI API KEY)
        load_dotenv('app.env')

        os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

        # Initialize model, memory, doc retriever
        self.llm = ChatOpenAI(model_name=model, max_tokens=max_tokens)
        self.memory = ConversationBufferWindowMemory(memory_key="chat_history", input_key='query',
                                                     output_key='result', return_messages=True,
                                                     k=n_conversations_memory)
        self.retriever = CustomRetriever(limit=limit, lang=lang, index=index)

        # Initialize chain
        self.qa_chain = RetrievalQA.from_chain_type(
            llm=self.llm,
            chain_type="stuff",
            retriever=self.retriever,
            return_source_documents=True,
            memory=self.memory
        )

    def get_answer_with_sources(self, query):
        """
        Takes a QA chain and a query as input. It uses the QA chain to get an answer to the query
        and returns a dictionary with the answer and a list of sources
        """
        result = self.qa_chain(query)
        return {'answer': result['result'],
                'sources': [doc.metadata['source'] for doc in result['source_documents']]}

    def clear_chain_memory(self):
        """
        Clears the memory buffer of a QA chain.
        """
        self.qa_chain.memory.buffer.clear()
