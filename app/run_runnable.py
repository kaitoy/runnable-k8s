import json
import warnings

from langchain_core.load import loads
from utils import deserialize_input, serialize_output

warnings.filterwarnings("ignore")

with open("./runnable.json", "r") as fp:
    llm = loads(json.load(fp))

with open("./runnable.input", "r") as fp:
    llm_input = deserialize_input(fp.read())

response = llm.invoke(llm_input)

print(serialize_output(response))
