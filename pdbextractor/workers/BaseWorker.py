from abc import ABC, abstractmethod


class BaseWorker(ABC):

    @classmethod
    @abstractmethod
    def process(cls):
        pass

    @staticmethod
    @abstractmethod
    def _parse(content):
        pass
