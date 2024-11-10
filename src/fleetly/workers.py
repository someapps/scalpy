import inspect

from fleetly.constants import END_OF_STREAM
from fleetly.node import Node


async def extract_worker(node: Node):
    if inspect.isasyncgenfunction(node.gen):
        gen = node.gen()
    else:
        gen = _gen_to_async(node.gen)

    async for item in gen:
        await _put_to_children(node, item)

    await _put_to_children(node)


async def action_tag(node, input_item):
    async for item in node.gen(input_item):
        await _put_to_children(node, item)


async def action_tg(node, input_item):
    for item in node.gen(input_item):
        await _put_to_children(node, item)


async def action_ta(node, input_item):
    item = await node.gen(input_item)
    await _put_to_children(node, item)


async def action_t(node, input_item):
    item = node.gen(input_item)
    await _put_to_children(node, item)


async def action_l(node, input_item):
    node.gen(input_item)


async def action_la(node, input_item):
    await node.gen(input_item)


async def transform_worker(node, is_loader: bool = False):
    action = _get_action(node, is_loader)
    active = len(node.in_)

    while True:
        input_item = await node.input_buffer.get()

        if input_item is END_OF_STREAM:
            active -= 1

            if active == 0:
                await _put_to_children(node)
                return
            continue

        await action(node, input_item)


async def _put_to_children(node, item=END_OF_STREAM):
    for out_ in node.out_:
        await out_.input_buffer.put(item)


async def _gen_to_async(gen):
    for item in gen():
        yield item


def _get_action(node, is_loader: bool):
    if is_loader:
        if inspect.iscoroutinefunction(node.gen):
            return action_la
        return action_l
    else:
        if inspect.isasyncgenfunction(node.gen):
            return action_tag
        if inspect.isgeneratorfunction(node.gen):
            return action_tg
        if inspect.iscoroutinefunction(node.gen):
            return action_ta
        return action_t
