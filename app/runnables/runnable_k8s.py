import base64
import pickle
import time
import uuid
from typing import Any, List, Optional, Type

from kubernetes import client, config, watch
from kubernetes.client import Configuration
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from langchain_core.exceptions import LangChainException
from langchain_core.load import dumps
from langchain_core.pydantic_v1 import BaseModel
from langchain_core.runnables.base import Runnable, RunnableSerializable
from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.graph import Graph
from langchain_core.runnables.utils import (ConfigurableFieldSpec, Input,
                                            Output, create_model)


class RunnableK8s(Runnable[Input, Output]):
    """Runnable that runs in a Kubernetes pod."""

    bound: Runnable[Input, Output]

    def __init__(
        self,
        *,
        bound: Runnable[Input, Output],
        k8s_config: Optional[Configuration] = None,
        k8s_namespace: str = "default",
        k8s_container_image: str = "kaitoy/runnable-k8s-ee",
        k8s_secret_name: str = "runnable-k8s-ee",
        k8s_delete_runner_pod: bool = True,
        # **kwargs: Any
    ) -> None:
        config.load_kube_config(client_configuration=k8s_config)
        self.bound = bound
        self.k8s_namespace = k8s_namespace
        self.k8s_container_image = k8s_container_image
        self.k8s_secret_name = k8s_secret_name
        self.k8s_delete_runner_pod = k8s_delete_runner_pod
        # super().__init__(**kwargs)

    @property
    def InputType(self) -> Input:
        return self.bound.InputType

    def get_input_schema(
        self, config: Optional[RunnableConfig] = None
    ) -> Type[BaseModel]:
        return create_model(
            self.get_name("Input"),
            __root__=(
                self.bound.get_input_schema(config),
                None,
            ),
        )

    @property
    def OutputType(self) -> Output:
        return self.bound.OutputType

    def get_output_schema(
        self, config: Optional[RunnableConfig] = None
    ) -> Type[BaseModel]:
        schema = self.bound.get_output_schema(config)
        return create_model(
            self.get_name("Output"),
            __root__=(
                schema,
                None,
            ),
        )

    @property
    def config_specs(self) -> List[ConfigurableFieldSpec]:
        return self.bound.config_specs

    def get_graph(self, config: Optional[RunnableConfig] = None) -> Graph:
        return self.bound.get_graph(config)

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
            or f"RunnableK8s<{self.bound.get_name()}>"
        )
        return super().get_name(suffix, name=name)

    def _run_pod(self) -> str:
        with client.ApiClient() as api_client:
            api_instance = client.CoreV1Api(api_client)
            uid = str(uuid.uuid4())
            pod_name = f'runnable-k8s-ee-{uid}'

            pod = client.V1Pod(
                metadata={
                    'name': pod_name,
                    'labels': {
                        'runnable-k8s.langchain/uuid': uid,
                    },
                },
                spec={
                    'containers': [{
                        'name': 'runnable-k8s-ee',
                        'image': self.k8s_container_image,
                        'stdin': True,
                        'envFrom': [{
                            'secretRef': {
                                'name': self.k8s_secret_name,
                            }
                        }],
                    }],
                    'restartPolicy': "Never",
                }
            )
            try:
                api_response = api_instance.create_namespaced_pod(self.k8s_namespace, pod)
            except ApiException as e:
                raise LangChainException(f'Failed to run pod: {self.k8s_namespace}/{pod_name}') from e

            try:
                w = watch.Watch()
                for event in w.stream(
                    func=api_instance.list_namespaced_pod,
                    namespace=self.k8s_namespace,
                    label_selector=f'runnable-k8s.langchain/uuid={uid}',
                    timeout_seconds=100
                ):
                    if event["object"].status.phase == "Running":
                        w.stop()
                        return pod_name
                    if event["type"] == "DELETED":
                        w.stop()
                        raise LangChainException(f'Runner pod {self.k8s_namespace}/{pod_name} was deleted before it started')
            except ApiException as e:
                raise LangChainException(f'Failed to watch pod: {self.k8s_namespace}/{pod_name}') from e

    def _connect_to_pod(self, pod_name: str, runnable_input: Input) -> str:
        input_base64 = self._serialize_runnable_input(runnable_input)
        runnable_base64 = self._serialize_runnable(self.bound)

        with client.ApiClient() as api_client:
            api_instance = client.CoreV1Api(api_client)
            try:
                resp = stream(
                    api_instance.connect_get_namespaced_pod_attach,
                    name=pod_name,
                    namespace=self.k8s_namespace,
                    stderr=True,
                    stdin=True,
                    stdout=True,
                    tty=True,
                    _preload_content=False
                )

                resp.write_stdin(runnable_base64)
                resp.write_stdin('\n\n')
                resp.write_stdin(input_base64)
                resp.write_stdin('\n\n')

                output_base64 = ''
                while resp.is_open():
                    resp.update(timeout=1)
                    if resp.peek_stdout():
                        line = resp.readline_stdout()

                        if line is None:
                            break
                        if line.startswith('#'):
                            continue
                        if line == '\n':
                            break

                        output_base64 += line.rstrip()
                    if resp.peek_stderr():
                        raise LangChainException(f'An error occurred in runner pod: {resp.read_stderr()}')
                    else:
                        time.sleep(1)

                if len(output_base64) == 0:
                    raise LangChainException(f"Runner pod didn't output anything: {self.k8s_namespace}/{pod_name}")
                return output_base64
            except ApiException as e:
                raise LangChainException(f'Failed to connect to runner pod: {self.k8s_namespace}/{pod_name}') from e

    def _serialize_runnable_input(self, runnable_input: Input) -> str:
        dump = pickle.dumps(runnable_input)
        return base64.b64encode(dump).decode()

    def _serialize_runnable(self, runnable: Runnable) -> str:
        return base64.b64encode(dumps(runnable).encode('utf-8')).decode()

    def _deserialize_runnable_output(self, serialized_output: str) -> Output:
        dump = base64.b64decode(serialized_output)
        return pickle.loads(dump)

    def _delete_pod(self, pod_name: str) -> None:
        with client.ApiClient() as api_client:
            api_instance = client.CoreV1Api(api_client)
            try:
                api_response = api_instance.delete_namespaced_pod(
                    name=pod_name,
                    namespace=self.k8s_namespace,
                )
            except ApiException as e:
                raise LangChainException(f'Failed to delete runner pod: {self.k8s_namespace}/{pod_name}') from e

    def _invoke(
        self,
        input: Input,
    ) -> Output:
        pod_name = self._run_pod()
        output_base64 = self._connect_to_pod(pod_name, input)
        if self.k8s_delete_runner_pod:
            self._delete_pod(pod_name)
        return self._deserialize_runnable_output(output_base64)

    def invoke(
        self,
        input: Input,
        config: Optional[RunnableConfig] = None,
        **kwargs: Any,
    ) -> Output:
        return self._call_with_config(self._invoke, input, config, **kwargs)
