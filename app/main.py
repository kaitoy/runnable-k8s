import json

from langchain_core.load import dumps
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser

prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "あなたはポケモンのエキスパートです。ユーザの質問に答えてください。",
    ),
    (
        "human",
        "質問: {input}",
    )
])
llm = ChatOpenAI(model="gpt-4o-mini")
chain = prompt | llm | StrOutputParser()

with open("./chain.json", "w") as fp:
    json.dump(dumps(chain, pretty=True), fp)
