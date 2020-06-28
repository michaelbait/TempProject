import importlib
from config.config import getoptions

options = getoptions()
logger = options['logger']


class PDBWorker:
    api_type = "org"
    workers = {
        "org": 'OrgWorker',
        "net": "NetWorker",
    }

    @classmethod
    def start(cls, args):
        if hasattr(args, 'type'):
            cls.api_type = getattr(args, 'type')
        if not cls.api_type:
            raise Exception("Ubdefined {}".format(cls.api_type))
        if cls.api_type == "all":
            cls._process_all()
        else:
            cls._process()

    @classmethod
    def _process(cls):
        wn = cls.workers[cls.api_type]
        worker = cls._class_factory(wn, wn)
        if not worker:
            raise SystemExit("No worker found {}".format(wn))
        worker.process()

    @classmethod
    def _process_all(cls):
        for wn in cls.workers.values():
            worker = cls._class_factory(wn, wn)
            if worker:
                worker.process()

    @classmethod
    def _class_factory(cls, module_name, class_name):
        result = None
        try:
            qn = "workers.{}".format(module_name, module_name)
            module = importlib.import_module(qn)
            result = getattr(module, class_name)
        except ModuleNotFoundError or AttributeError or ImportError as e:
            logger.error(e)
        return result
