import asyncio
from dataclasses import dataclass
from typing import List

from blockcrawler.core.bus import (
    Consumer,
    Producer,
    DataBus,
    ConsumerError,
    DataPackage,
    Transformer,
)


class SimpleDataBus(DataBus):
    def __init__(self) -> None:
        self.__consumers: List[Consumer] = []

    async def send(self, data_package: DataPackage):
        for consumer in self.__consumers:
            await consumer.receive(data_package)

    async def register(self, consumer: Consumer):
        self.__consumers.append(consumer)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@dataclass
class SimpleIntegerDataPackage(DataPackage):
    value: int


@dataclass
class SimpleStringDataPackage(DataPackage):
    value: str


class SimpleConsumer(Consumer):
    async def receive(self, data_package: DataPackage):
        if isinstance(data_package, SimpleStringDataPackage):
            try:
                print(data_package.value)
            except Exception as e:
                raise ConsumerError(
                    f"Unable to print value {data_package.value}",
                    e,
                )


class SimpleProducer(Producer):
    async def __call__(self, data_bus: DataBus):
        for i in range(1_000, 11_000, 1_000):
            await data_bus.send(SimpleIntegerDataPackage(i))
            await asyncio.sleep(0.1)


class SimpleTransformer(Transformer):
    async def receive(self, data_package: DataPackage):
        if isinstance(data_package, SimpleIntegerDataPackage):
            str_value = f"{data_package.value:,}"
            await self._get_data_bus().send(SimpleStringDataPackage(str_value))


async def run():
    producer = SimpleProducer()
    consumer = SimpleConsumer()
    async with SimpleDataBus() as data_bus:
        await data_bus.register(consumer)
        transformer = SimpleTransformer(data_bus)
        await data_bus.register(transformer)
        await producer(data_bus)


if __name__ == "__main__":
    asyncio.run(run())
