import base64
import pickle
import sys
import time
import warnings

from langchain_core.load import loads
from langchain_core.runnables.base import Runnable
from langchain_core.runnables.utils import Input, Output

warnings.filterwarnings("ignore")

def read_input() -> str:
    data = ''
    while True:
        line = sys.stdin.readline()
        if line == '\n':
            break
        data += line.rstrip()
        time.sleep(1)
    print(f'# {data}\n')
    return data

def deserialize_runnable(serialized_runnable: str) -> Runnable:
    dump = base64.b64decode(serialized_runnable)
    return loads(dump.decode('utf-8'))

def deserialize_input(serialized_input: str) -> Input:
    dump = base64.b64decode(serialized_input)
    return pickle.loads(dump)

def serialize_output(runnable_output: Output) -> str:
    dump = pickle.dumps(runnable_output)
    return base64.b64encode(dump).decode()

runnable_base64 = read_input()
runnable =deserialize_runnable(runnable_base64)

input_base64 = read_input()
runnable_input = deserialize_input(input_base64)

output = runnable.invoke(runnable_input)

print(serialize_output(output))
print('\n')
