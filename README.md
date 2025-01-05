RunnableK8s
============

RunnableK8s is a LangChain Runnable implementation that wraps a Runnable and runs it in Kubernetes.


How it Works
------------

![runnable-k8s.png](https://github.com/kaitoy/runnable-k8s/blob/master/doc/images/runnable-k8s.png?raw=true)

How to Use
----------

Sample code:

```python
import sys

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from runnables.runnable_k8s import RunnableK8s


prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are an expert of Pokemon. Please answer the question.",
    ),
    (
        "human",
        "Question: {input}",
    )
])
llm = ChatOpenAI(model="gpt-4o-mini")

chain = prompt | RunnableK8s(bound=llm) | StrOutputParser()

print(chain.invoke({'input': sys.argv[1]}))
```

Usage example of this sample:

![runnable-k8s.gif](https://github.com/kaitoy/runnable-k8s/blob/master/doc/images/runnable-k8s.gif?raw=true)
