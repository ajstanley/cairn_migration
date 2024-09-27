import CairnUtilities as CA
import FoxmlWorker as FW

class CairnProcessor:

    def __init__(self):
        self.CA = CA.CairnUtilities()
        self.objectStore = '/usr/local/fedora/data/objectStore'
        self.stream_map = {
            'islandora:sp_large_image_cmodel': ['OBJ', 'JPG']
        }

    def process_collection(self, table, collection):
        collection_map = self.CA.get_collection_pid_model_map(table, collection)
        current_number = 1
        for pid, model in collection_map.items():
            item_number = str(current_number).zfill(3)
            foxml_file = self.CA.dereference(pid)
            copy_streams = []
            with open(f"{self.objectStore}/{foxml_file}", 'r') as file:
                foxml = file.read()
            fw = FW.FWorker(foxml)
            dublin_core = fw.get_modified_dc()
            all_files = fw.get_datastreams()
            for stream in self.stream_map[model]:
                if stream in all_files:
                    copy_streams.append(all_files[stream])







