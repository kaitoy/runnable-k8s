import base64
import json
import pickle
import sys
import warnings

from langchain_core.load import dumps
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

warnings.filterwarnings("ignore")

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

with open("./llm.json", "w") as fp:
    json.dump(dumps(llm, pretty=True), fp)

with open("./llm.input", "w") as fp:
    llm_input = prompt.invoke({"input": sys.argv[1]})
    llm_input_dump = pickle.dumps(llm_input)
    llm_input_base64 = base64.b64encode(llm_input_dump)
    fp.write(llm_input_base64.decode())
