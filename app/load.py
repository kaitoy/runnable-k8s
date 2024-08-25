import json
import sys

from langchain_core.load import loads

with open("./chain.json", "r") as fp:
    chain = loads(json.load(fp))

response = chain.invoke({
    "input": sys.argv[1],
})
print(response)
