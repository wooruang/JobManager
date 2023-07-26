class SingletonInstance:
    __instance = None

    @classmethod
    def __getInstance(cls, *args, **kargs):
        return cls.__instance

    @classmethod
    def instance(cls, *args, **kargs):
        cls.__instance = cls(*args, **kargs)
        cls.instance = cls.__getInstance
        return cls.__instance
