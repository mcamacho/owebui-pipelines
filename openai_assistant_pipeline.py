"""
title: OpenAI Assistant Pipeline
requirements: 'psycopg2-binary'
"""

from typing import List, Union, Generator, Iterator
from schemas import OpenAIChatMessage
from pydantic import BaseModel
import os
import requests

from openai import OpenAI, AssistantEventHandler
from typing_extensions import override
import base64
import io
# get postgrespw from environment variable
POSTGRESPW = os.getenv("POSTGRESPW", "")
# get the OpenAI API key from the environment variable
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
client = OpenAI(api_key=OPENAI_API_KEY)

from sqlalchemy import create_engine, Column, Integer, String, select
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

Base = declarative_base()

class UsernameMapping(Base):
    __tablename__ = 'username_mapping'
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    account_id = Column(String, nullable=False)

class PostgresDB:
  def __init__(self, database_url):
    self.database_url = database_url
    self.engine = create_engine(self.database_url)
    self.Session = sessionmaker(bind=self.engine)

  def create_tables(self):
    Base.metadata.create_all(self.engine)

  def get_session(self):
    return self.Session()

  def execute_query(self, query):
    with self.get_session() as session:
      try:
        result = session.execute(query)
        session.commit()
        return result.fetchall()
      except SQLAlchemyError as e:
        print(f"The error '{e}' occurred")
        session.rollback()
        return None

  def add_object(self, obj):
    with self.get_session() as session:
      try:
        session.add(obj)
        session.commit()
      except SQLAlchemyError as e:
        print(f"The error '{e}' occurred")
        session.rollback()

database_url = f"postgresql://postgres:${POSTGRESPW}@pg-database-1.cluster-ccte28sshn44.us-east-1.rds.amazonaws.com:5432/postgres"
db = PostgresDB(database_url)
# Function to get account_id by username
def get_account_id_by_username(username):
    query = select(UsernameMapping.account_id).where(UsernameMapping.username == username)
    result = db.execute_query(query)
    if result:
        return result[0][0]  # Return the first account_id found
    else:
        print("No account found for the given email.")
        return None

class EventHandler(AssistantEventHandler):
    @override
    def on_text_created(self, text) -> None:
        print(f"\nassistant > ", end="", flush=True)

    @override
    def on_tool_call_created(self, tool_call):
        print(f"\nassistant > {tool_call.type}\n", flush=True)

    @override
    def on_message_done(self, message) -> None:
        # print a citation to the file searched
        message_content = message.content[0].text
        annotations = message_content.annotations
        citations = []
        for index, annotation in enumerate(annotations):
            message_content.value = message_content.value.replace(
                annotation.text, f"[{index}]"
            )
            if file_citation := getattr(annotation, "file_citation", None):
                cited_file = client.files.retrieve(file_citation.file_id)
                citations.append(f"[{index}] {cited_file.filename}")
        return message_content.value
        # print(message_content.value)
        # print("\n".join(citations))

class Pipeline:
    class Valves(BaseModel):
        OPENAI_API_KEY: str = ""
        pass

    def __init__(self):
        # Optionally, you can set the id and name of the pipeline.
        # Best practice is to not specify the id so that it can be automatically inferred from the filename, so that users can install multiple versions of the same pipeline.
        # The identifier must be unique across all pipelines.
        # The identifier must be an alphanumeric string that can include underscores or hyphens. It cannot contain spaces, special characters, slashes, or backslashes.
        # self.id = "openai_pipeline"
        self.name = "OpenAI Assistant Pipeline"
        self.valves = self.Valves(
            **{
                "OPENAI_API_KEY": os.getenv(
                    "OPENAI_API_KEY", ""
                )
            }
        )
        self.assistants = client.beta.assistants.list()
        pass

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is stopped.
        print(f"on_shutdown:{__name__}")
        pass

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        # This is where you can add your custom pipelines like RAG.
        print(f"pipe:{__name__}")
        MODEL = "gpt-4o"
        payload = {**body, "model": MODEL}
        username = payload["user"]["name"]
        account_id = get_account_id_by_username(username)
        print(messages)
        print(user_message)
        print(payload)

        assistant = None
        for assistantI in self.assistants.data:
          print(assistantI)
          if assistantI.name == f"{account_id} assistant":
            print("found assistant")
            assistant = assistantI

        thread = client.beta.threads.create(messages=messages)

        try:
            with client.beta.threads.runs.stream(
                thread_id=thread.id,
                assistant_id=assistant.id,
                # instructions="Please address the user as Jane Doe. The user has a premium account.",
                event_handler=EventHandler(),
            ) as stream:
                stream.until_done()
            # run = client.beta.threads.runs.create_and_poll(
            #     thread_id=thread.id, assistant_id=assistant.id
            # )

            # messages = list(client.beta.threads.messages.list(thread_id=thread.id, run_id=run.id))

            # message_content = messages[0].content[0].text
            # annotations = message_content.annotations
            # citations = []
            # for index, annotation in enumerate(annotations):
            #     message_content.value = message_content.value.replace(annotation.text, f"[{index}]")
            #     if file_citation := getattr(annotation, "file_citation", None):
            #         cited_file = client.files.retrieve(file_citation.file_id)
            #         citations.append(f"[{index}] {cited_file.filename}")

            # print(message_content.value)
            # return message_content.value
            # print("\n".join(citations))
        except Exception as e:
            return f"Error: {e}"
