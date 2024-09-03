from typing import (
    Any,
    AsyncIterator,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
)

import pickle
import base64

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from langchain_core.runnables.base import RunnableSerializable
from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.utils import Input, Output

config.load_kube_config()

class RunnableK8s(RunnableSerializable[Input, Output]):
    """Runnable that picks keys from Dict[str, Any] inputs.

    RunnablePick class represents a Runnable that selectively picks keys from a
    dictionary input. It allows you to specify one or more keys to extract
    from the input dictionary. It returns a new dictionary containing only
    the selected keys.

    Parameters:
        keys (Union[str, List[str]]): A single key or a list of keys to pick from
            the input dictionary.

    Example :
        .. code-block:: python

            from langchain_core.runnables.passthrough import RunnablePick

            input_data = {
                'name': 'John',
                'age': 30,
                'city': 'New York',
                'country': 'USA'
            }

            runnable = RunnablePick(keys=['name', 'age'])

            output_data = runnable.invoke(input_data)

            print(output_data)  # Output: {'name': 'John', 'age': 30}
    """

    def __init__(
        self,
        k8s_host: str,
        k8s_namespace: str = "default",
        **kwargs: Any
    ) -> None:
        self.k8s_config = client.Configuration(host=k8s_host)
        self.k8s_namespace = k8s_namespace
        super().__init__(**kwargs)

    @classmethod
    def is_lc_serializable(cls) -> bool:
        return True

    @classmethod
    def get_lc_namespace(cls) -> List[str]:
        """Get the namespace of the langchain object."""
        return ["langchain", "schema", "runnable"]

    def get_name(
        self, suffix: Optional[str] = None, *, name: Optional[str] = None
    ) -> str:
        name = (
            name
            or self.name
            or f"RunnableK8s<{self.config.host}>"  # noqa: E501
        )
        return super().get_name(suffix, name=name)

    def _run_pod(self, input: Input) -> Any:
        input_dump = pickle.dumps(input)
        input_base64 = base64.b16encode(input_dump)

        with client.ApiClient(self.k8s_config) as api_client:
            api_instance = client.CoreV1Api(api_client)
            pod = client.V1Pod(spec={
                'containers': [{
                    'image': image,
                    'name': 'langchain',
                }],
                'restartPolicy': "Never",
            })
            try:
                api_response = api_instance.create_namespaced_pod(self.k8s_namespace, pod)
            except ApiException as e:
                print("Exception when calling CoreApi->get_api_versions: %s\n" % e)

    def _invoke(
        self,
        input: Dict[str, Any],
    ) -> Dict[str, Any]:
        return self._run_pod(input)

    def invoke(
        self,
        input: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return self._call_with_config(self._invoke, input, config, **kwargs)

    async def _ainvoke(
        self,
        input: Dict[str, Any],
    ) -> Dict[str, Any]:
        return self._pick(input)

    async def ainvoke(
        self,
        input: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return await self._acall_with_config(self._ainvoke, input, config, **kwargs)

    def _transform(
        self,
        input: Iterator[Dict[str, Any]],
    ) -> Iterator[Dict[str, Any]]:
        for chunk in input:
            picked = self._pick(chunk)
            if picked is not None:
                yield picked

    def transform(
        self,
        input: Iterator[Dict[str, Any]],
        config: Optional[RunnableConfig] = None,
        **kwargs: Any,
    ) -> Iterator[Dict[str, Any]]:
        yield from self._transform_stream_with_config(
            input, self._transform, config, **kwargs
        )

    async def _atransform(
        self,
        input: AsyncIterator[Dict[str, Any]],
    ) -> AsyncIterator[Dict[str, Any]]:
        async for chunk in input:
            picked = self._pick(chunk)
            if picked is not None:
                yield picked

    async def atransform(
        self,
        input: AsyncIterator[Dict[str, Any]],
        config: Optional[RunnableConfig] = None,
        **kwargs: Any,
    ) -> AsyncIterator[Dict[str, Any]]:
        async for chunk in self._atransform_stream_with_config(
            input, self._atransform, config, **kwargs
        ):
            yield chunk

    def stream(
        self,
        input: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
        **kwargs: Any,
    ) -> Iterator[Dict[str, Any]]:
        return self.transform(iter([input]), config, **kwargs)

    async def astream(
        self,
        input: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
        **kwargs: Any,
    ) -> AsyncIterator[Dict[str, Any]]:
        async def input_aiter() -> AsyncIterator[Dict[str, Any]]:
            yield input

        async for chunk in self.atransform(input_aiter(), config, **kwargs):
            yield chunk
