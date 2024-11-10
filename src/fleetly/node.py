import asyncio


class Node:

    def __init__(
            self,
            gen,
            etl=None,
            buffer_size: int = 3,
    ):
        self.gen = gen
        self.etl = etl
        self.in_ = []
        self.out_ = []

        self.input_buffer = asyncio.Queue(buffer_size)

    @property
    def name(self):
        if hasattr(self.gen, '__name__'):
            return self.gen.__name__
        return self.gen.__class__.__name__

    def __rshift__(self, other):
        other = self.etl >> other

        self.out_.append(other)
        other.in_.append(self)
        return other

    def __repr__(self):
        return f'Node({self.name}, children: {self.out_})'
