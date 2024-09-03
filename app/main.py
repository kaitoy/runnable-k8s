import json
import subprocess
import sys
import warnings

from langchain_core.load import dumps
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from utils import deserialize_output, serialize_input

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

with open("./runnable.json", "w") as fp:
    json.dump(dumps(llm, pretty=True), fp)

with open("./runnable.input", "w") as fp:
    llm_input = prompt.invoke({"input": sys.argv[1]})
    fp.write(serialize_input(llm_input))

result = subprocess.run(['python', './app/run_runnable.py'], capture_output=True, text=True)

response = StrOutputParser().invoke(deserialize_output(result.stdout))
print(response)
