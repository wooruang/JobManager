import abc


class BaseBlob(abc.ABC):

    @abc.abstractmethod
    def store(self, path, blob_bytes):
        pass

    @abc.abstractmethod
    def load(self, path):
        pass
