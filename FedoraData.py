from pathlib import Path
from urllib.parse import unquote
import FoxmlWorker


class FedoraDataWorker:
    def __init__(self):
        self.objectStore = '/usr/local/fedora/data/objectStore/'
        self.test = '/Users/MacIntosh/islandora_workbench'

    def get_all_pids(self, namespace=''):
        wildcard = '*/*'
        if namespace:
            wildcard = f'*/*{namespace}*'
        all_pids = []
        for p in Path(self.objectStore).rglob(wildcard):
            pid = unquote(p.name).replace('info:fedora/', '')
            all_pids.append(pid)
        return all_pids

    def get_namespaces(self):
        namespaces = []
        for pid in self.get_all_pids():
            namespace = pid.split(':')[0]
            if namespace not in namespaces:
                namespaces.append(namespace)
        return namespaces


FD = FedoraDataWorker()
FD.get_all_pids()
