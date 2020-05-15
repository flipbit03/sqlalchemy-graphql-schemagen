from enum import Enum
from typing import Dict, Type, Any

from abc import ABC, abstractmethod

from graphql import ResolveInfo


class HookOperation(Enum):
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"


class PreHookResult(Enum):
    CONTINUE = 1
    STOP = 2


class SchemaGenHookException(Exception):
    pass


# noinspection PyMethodParameters
class SchemaGenHooksBase(ABC):
    @staticmethod
    @abstractmethod
    def pre(parent, info: ResolveInfo, **kwargs) -> PreHookResult:
        """
        root = root value from Schema
        info = gathers additional request oriented-stuff, like info.context
        **kwargs = whatever else would be sent to the resolve/mutate function.
        """
        return PreHookResult.CONTINUE

    @staticmethod
    @abstractmethod
    def post(parent, info: ResolveInfo, func_retval, **kwargs) -> Any:
        """
        root = root value from Schema
        info = gathers additional request oriented-stuff, like info.context
        **kwargs = whatever else would be sent to the resolve/mutate function.
        kwargs['__func_retval'] contains the decorated function's return value, so you can postprocess it.

        OBS: anything that gets returned here will substitute the decorated function's return value
        """
        pass

    def __init__(self, func_to_be_decorated):
        self.func_to_be_decorated = func_to_be_decorated

    def __call__(self, *args, **kwargs):
        # Call pre-hook
        pre_result = self.pre(*args, **kwargs)
        if pre_result == PreHookResult.STOP:
            raise SchemaGenHookException("Pre_Hook called STOP")

        # Call Function
        decorated_func_retval = self.func_to_be_decorated(*args, **kwargs)

        # Call post-hook with the original arguments and the decorated function's returned value.
        post_retval = self.post(*args, decorated_func_retval, **kwargs)
        if post_retval:
            # post wants to substitute the original function's data with its own.
            return post_retval

        # No 'post' hook data, return the original function's data, unmodified.
        return decorated_func_retval


# Custom Type
HookDictType = Dict[HookOperation, Type[SchemaGenHooksBase]]
