import base64
import pickle

from langchain_core.runnables.utils import Input, Output


def serialize_input(runnable_input: Input) -> str:
    dump = pickle.dumps(runnable_input)
    return base64.b64encode(dump).decode()

def deserialize_input(serialized_input: str) -> Input:
    dump = base64.b64decode(serialized_input)
    return pickle.loads(dump)

def serialize_output(runnable_output: Output) -> str:
    return serialize_input(runnable_output)

def deserialize_output(serialized_output: str) -> Output:
    return deserialize_input(serialized_output)
