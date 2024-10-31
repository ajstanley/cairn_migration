#!/usr/bin/env python3

import csv
import re
import shutil
import time
from pathlib import Path

import lxml.etree as ET

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
            'ir:thesisCModel': ['PDF', 'FULL_TEXT'],
            'islandora:sp_videoCModel': ['OBJ', 'PDF', 'MODS'],
        }
        self.ca = CA.CairnUtilities()
        self.mods_xsl = 'assets/xsl/stfx_mods_to_dc.xsl'
        self.export_dir = '/usr/local/fedora/cairn_migration/outputs'
        self.mimemap = {"image/jpeg": ".jpg",
                        "image/jp2": ".jp2",
                        "image/png": ".png",
                        "image/tiff": ".tif",
                        "text/xml": ".xml",
                        "test/plain": ".txt",
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
            transform = input("Transform DC from MODS?\ny/n\n")
            if transform not in ['y', 'n']:
                print("Try again\n")
            self.process_collection(table, collection_pid, transform)

    def process_hierarchy(self, namespace):
        collection_data = self.ca.get_collection_details(namespace)
        headers = ['pid', 'label']
        for collection_pid, parent_pid in collection_data.items():
            foxml_file = self.ca.dereference(collection_pid)
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
        collection_map = self.ca.get_collection_pid_model_map(table, collection)
        # Build collection directory
        archive = collection.replace(':', '_')
        archive_path = f"{self.export_dir}/{archive}"
        Path(archive_path).mkdir(parents=True, exist_ok=True)
        current_number = 1
        # Process each PID in collectipn
        for pid, model in collection_map.items():
            item_number = str(current_number).zfill(4)
            foxml_file = self.ca.dereference(pid)
            copy_streams = {}
            foxml = f"{self.objectStore}/{foxml_file}"
            try:
                fw = FW.FWorker(foxml)
            except:
                print(f"No record found for {pid}")
                continue
            dublin_core = None
            if transform_mods == 'y':
                files_info = fw.get_file_data()
                mods_path = f"{self.datastreamStore}/{self.ca.dereference(files_info['MODS']['filename'])}"
                dom = ET.parse(mods_path)
                xslt = ET.parse(self.mods_xsl)
                transform = ET.XSLT(xslt)
                dc = transform(dom)
                root = ET.Element("dublin_core")
                for candidate in dc.iter():
                    if not candidate.text:
                        continue
                    value = candidate.text.replace("\\,", '%%%')
                    tag = re.sub(r'{.*}', '', candidate.tag)
                    qualifier = 'none'
                    if tag == 'dc':
                        continue
                    if '.' in tag:
                        [tag, qualifier] = tag.split('.')

                    ET.SubElement(root, "dcvalue", element=tag,
                                  qualifier=qualifier).text = value.replace('%%%', ',')
                ET.indent(root, space="\t", level=0)
                dublin_core = ET.tostring(root, encoding='unicode')
            if not dublin_core:
                dublin_core = fw.get_modified_dc()
            all_files = fw.get_file_data()
            for entry, file_data in all_files.items():
                if entry in self.stream_map[model]:
                    copy_streams[
                        file_data[
                            'filename']] = f"{pid.replace(':', '_')}_{entry}{self.mimemap[file_data['mimetype']]}"
            path = f"{archive_path}/item_{item_number}"
            # Build directory
            Path(path).mkdir(parents=True, exist_ok=True)
            with open(f'{path}/dublin_core.xml', 'w') as f:
                f.write(dublin_core)
            with open(f'{path}/contents', 'w') as f:
                for source, destination in copy_streams.items():
                    stream_to_copy = self.ca.dereference(source)
                    shutil.copy(f"{self.datastreamStore}/{stream_to_copy}", f"{path}/{destination}")
                    f.write(f"{destination}\n")
            print(f"item_{item_number}")
            current_number += 1
        print(f"Zipping files into {archive}.zip")
        shutil.make_archive(f"{self.export_dir}/{archive}", 'zip', f"{self.export_dir}/{archive}")
        shutil.rmtree(f"{self.export_dir}/{archive}")
        print(f"Processed {int(item_number)} entries in {round(time.time() - self.start, 2)} seconds")

    # Temp function for testing only.
    def temp_transform(self):

        dom = ET.parse('assets/MODS/stfx_11134.xml')
        xslt = ET.parse('assets/xsl/stfx_mods_to_dc.xsl')
        transform = ET.XSLT(xslt)
        dublin_core = transform(dom)
        root = ET.Element("dublin_core")
        for candidate in dublin_core.iter():
            value = candidate.text.replace("\\,", '%%%')
            tag = re.sub(r'{.*}', '', candidate.tag)
            qualifier = 'none'
            if tag == 'dc':
                continue
            if '.' in tag:
                [tag, qualifier] = tag.split('.')

            ET.SubElement(root, "dcvalue", element=tag,
                          qualifier=qualifier).text = value.replace('%%%', ',')
        ET.indent(root, space="\t", level=0)
        print(ET.tostring(root, encoding='unicode'))

        f = open("assets/demofile3.txt", "w")
        f.write(str(dublin_core))
        f.close()
        print(dublin_core)


CP = CairnProcessor()
CP.temp_transform()
