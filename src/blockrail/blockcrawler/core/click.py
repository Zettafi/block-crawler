from click import ParamType, Parameter, Context
import typing as t

from blockrail.blockcrawler.core.entities import BlockChain


class BlockChainParamType(ParamType):
    name = "BlockChain"

    def convert(
        self, value: t.Any, param: t.Optional["Parameter"], ctx: t.Optional["Context"]
    ) -> t.Any:
        options = list()
        for item in BlockChain:
            if item.value == value:
                return item
            else:
                options.append(item.value)
        self.fail(f"Invalid blockchain \"{value}\"! Must be one of: {', '.join(options)}")
