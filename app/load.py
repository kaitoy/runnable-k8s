import base64
import json
import pickle
import warnings

from langchain_core.load import loads
from langchain_core.output_parsers import StrOutputParser

warnings.filterwarnings("ignore")

with open("./llm.json", "r") as fp:
    llm = loads(json.load(fp))

with open("./llm.input", "r") as fp:
    llm_input_base64 = base64.b64decode(fp.read())
    llm_input = pickle.loads(llm_input_base64)

chain = llm | StrOutputParser()
response = chain.invoke(llm_input)
print(response)
