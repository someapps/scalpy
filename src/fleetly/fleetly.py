import asyncio as asyncio
import inspect

from fleetly import workers
from fleetly.node import Node


class Fleetly:

    def __init__(self):
        self.nodes = dict()

    def __getitem__(self, output):
        return self.nodes[output]

    def __rshift__(self, other):
        if not isinstance(other, Node):
            if other in self.nodes:
                return self.nodes[other]

            other = Node(other, self)

        self.nodes[other.gen] = other
        return other

    async def run(self):
        tasks = self.get_tasks(self.nodes.values())

        await asyncio.gather(*tasks)

    def make_diagram(self, filename: str = 'fleetly.puml'):
        from collections import deque

        queue = deque(self.nodes.values())
        nodes = set()
        lines_objs = []
        lines_rels = []

        while queue:
            node = queue.popleft()

            if node in nodes:
                continue

            nodes.add(node)
            lines_objs.append(f'entity {node.name} {{\n')
            lines_objs.append(f'  {self.get_types(node.gen)}\n')
            lines_objs.append('}\n')

            if len(node.out_) > 0:
                for out_ in node.out_:
                    lines_rels.append(f'{node.name} --> {out_.name}\n')
                    queue.append(out_)

        with open(filename, 'w') as f:
            f.write('@startuml\n')
            f.write('header fleetly\n\n')
            f.writelines(lines_objs)
            f.write('\n')
            f.writelines(lines_rels)
            f.write('@enduml\n')

    def get_tasks(self, nodes):
        tasks = []

        for node in nodes:
            if len(node.in_) == 0:
                worker = workers.extract_worker(node)

            elif len(node.out_) == 0:
                worker = workers.transform_worker(node, True)

            else:
                worker = workers.transform_worker(node, False)

            task = asyncio.create_task(worker)
            tasks.append(task)

        return tasks

    def get_types(self, obj):
        if inspect.isasyncgenfunction(obj):
            return 'async generator'
        if inspect.isgeneratorfunction(obj):
            return 'generator'
        if inspect.iscoroutinefunction(obj):
            return 'coroutine'
        return 'function'
