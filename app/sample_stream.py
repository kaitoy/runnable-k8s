import sys

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from runnables.runnable_k8s import RunnableK8s


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

chain = prompt | RunnableK8s(bound=llm) | StrOutputParser()

for output in chain.stream({'input': sys.argv[1]}):
    print(f'{output}/', end='', flush=True)
print()
