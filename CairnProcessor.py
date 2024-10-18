#!/usr/bin/env python3

import shutil
import time
from pathlib import Path
import csv
import CairnUtilities as CA
import FoxmlWorker as FW


class CairnProcessor:

    def __init__(self):
        self.objectStore = '/usr/local/fedora/data/objectStore'
        self.datastreamStore = '/usr/local/fedora/data/datastreamStore'
        self.stream_map = {
            'islandora:sp_pdf': ['OBJ', 'PDF', 'MODS'],
            'islandora:sp_large_image_cmodel': ['OBJ', 'JPG', 'MODS'],
            'ir:citationCModel': [],
            'r:thesisCModel': ['PDF', 'FULL_TEXT']
        }
        self.ca = CA.CairnUtilities()

        self.export_dir = '/home/astanley/export'
        self.mimemap = {"image/jpeg": ".jpg",
                        "image/jp2": ".jp2",
                        "image/png": ".png",
                        "image/tiff": ".tif",
                        "text/xml": ".xml",
                        "application/pdf": ".pdf",
                        "application/xml": ".xml"}
        self.start = time.time()


    def selector(self):
         selection = input("Process \n 1. Hierarchy\n 2. Collections\n")
         if selection not in ["1", "2"]:
             print("Try again\n")
             self.selector()
         if selection == "1":
             namespace = input("Hierarchy namespace?\n")
             self.process_hierarchy(namespace)
         if selection == "2":
             table = input("Table name?\n")
             collection_pid = input("Collection pid?\n")
             transform = input("Transform DC from MODS?\n")
             self.process_collection(table, collection_pid, transform)

    def process_hierarchy(self, namespace):
        collection_data = self.ca.get_collection_details(namespace)
        headers = ['pid', 'label']
        for collection_pid,  parent_pid in collection_data.items():
            foxml_file = self.CA.dereference(collection_pid)
            foxml = f"{self.objectStore}/{foxml_file}"
            fw = FW.FWorker(foxml)
            if fw.properties['state'] != 'Active':
                continue
            row = {}
            writer = csv.DictWriter(f"{namespace}_collections.csv", fieldnames=headers)
            writer.writeheader()
            row['pid'] = collection_pid
            row['label'] = fw.properties['label']
            writer.writerow(row)

    def process_collection(self, table, collection, transform_mods):
        collection_map = self.CA.get_collection_pid_model_map(table, collection)
        # Build collection directory
        path = f"{self.export_dir}/{collection.replace(':', '_')}"
        Path(path).mkdir(parents=True, exist_ok=True)
        current_number = 1
        # Process each PID in collectipn
        for pid, model in collection_map.items():
            item_number = str(current_number).zfill(4)
            foxml_file = self.CA.dereference(pid)
            copy_streams = {}
            foxml = f"{self.objectStore}/{foxml_file}"
            fw = FW.FWorker(foxml)
            dublin_core = None
            if transform_mods:
                dublin_core = FW.transform_mods_to_dc()
            if not dublin_core:
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
            with open(f'{path}/contents', 'w') as f:
                for source, destination in copy_streams.items():
                    stream_to_copy = self.CA.dereference(source)
                    shutil.copy(f"{self.datastreamStore}/{stream_to_copy}", f"{path}/{destination}")
                    f.write(f"{destination}\n")
            print(f"item_{item_number}")
            current_number += 1

        print(f"Processed {int(item_number)} entries in {round(time.time() - self.start, 2)} seconds")

CP = CairnProcessor()
CP.selector()
