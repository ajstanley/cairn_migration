#!/usr/bin/env python3

import shutil
from pathlib import Path
import time

import CairnUtilities as CA
import FoxmlWorker as FW


class CairnProcessor:

    def __init__(self):
        self.CA = CA.CairnUtilities()
        self.objectStore = '/usr/local/fedora/data/objectStore'
        self.datastreamStore = '/usr/local/fedora/data/datastreamStore'
        self.stream_map = {
            'islandora:sp_large_image_cmodel': ['OBJ', 'JPG']
        }
        self.export_dir = '/home/astanley/export'
        self.mimemap = {"image/jpeg": ".jpg",
                        "image/jp2": ".jp2",
                        "image/png": ".png",
                        "image/tiff": ".tif",
                        "application/xml": ".xml"}
        self.start = time.time()

    def process_collection(self, table, collection):
        collection_map = self.CA.get_collection_pid_model_map(table, collection)
        current_number = 1
        # Process each PID in collectipn
        for pid, model in collection_map.items():
            item_number = str(current_number).zfill(3)
            foxml_file = self.CA.dereference(pid)
            copy_streams = {}
            foxml = f"{self.objectStore}/{foxml_file}"
            fw = FW.FWorker(foxml)
            dublin_core = fw.get_modified_dc()
            all_files = fw.get_file_data()
            for entry, file_data in all_files.items():
                if entry in self.stream_map[model]:
                    copy_streams[
                        file_data[
                            'file_name']] = f"{pid.replace(':', '_')}_{entry}{self.mimemap[file_data['mimetype']]}"
            path = f"{self.export_dir}/item_{item_number}"
            # Build directory
            Path(path).mkdir(parents=True, exist_ok=True)
            with open(f'{path}/dublin_core.xml', 'w') as f:
                f.write(dublin_core)
            with open(f'{path}/manifest', 'w') as f:
                for source, destination in copy_streams.items():
                    stream_to_copy = self.CA.dereference(source)
                    shutil.copy(f"{self.datastreamStore}/{stream_to_copy}", f"{path}/{destination}")
                    f.write(f"{destination}\n")
            print(f"item_{item_number}")
            current_number += 1

        print (f"Processed {item_number} entries in {time.time() - self.start} seconds")


table = input("Table name?\n")
collection_pid = input("Collection pid?\n")
CP = CairnProcessor()
CP.process_collection(table, collection_pid)
