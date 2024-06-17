# Filename: local_dev.py
from ray import serve
from ray.serve.handle import DeploymentHandle
from starlette.requests import Request


@serve.deployment
class Doubler:
    def double(self, s: str):
        return s + " " + s


@serve.deployment
class HelloDeployment:
    def __init__(self, doubler: DeploymentHandle):
        self.doubler = doubler

    async def say_hello_twice(self, name: str):
        return await self.doubler.double.remote(f"Hello, {name}!")

    async def __call__(self, request: Request):
        return await self.say_hello_twice(request.query_params["name"])


app = HelloDeployment.bind(Doubler.bind())
