#!/usr/bin/env python3

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
            'islandora:sp_pdf': ['OBJ', 'PDF'],
            'islandora:sp_large_image_cmodel': ['OBJ'],
            'islandora:sp_basic_image': ['OBJ'],
            'ir:citationCModel': ['FULL_TEXT'],
            'ir:thesisCModel': ['OBJ', 'PDF', 'FULL_TEXT'],
            'islandora:sp_videoCModel': ['OBJ', 'PDF'],
            'islandora:newspaperIssueCModel': ['OBJ', 'PDF'],
            'islandora:sp-audioCModel': ['OBJ'],
        }
        self.ca = CA.CairnUtilities()
        self.mods_xsl = '/usr/local/fedora/cairn_migration/assets/islandora-dspace/xsl-transforms/udm_research_mods_to_dc.xsl'
        self.export_dir = '/usr/local/fedora/cairn_migration/outputs'
        self.mimemap = {"image/jpeg": ".jpg",
                        "image/jp2": ".jp2",
                        "image/png": ".png",
                        "image/tiff": ".tif",
                        "text/xml": ".xml",
                        "text/plain": ".txt",
                        "application/pdf": ".pdf",
                        "application/xml": ".xml",
                        "audio/x-wav": ".wav",
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
                        "application/octet-stream": ".bib",
                        "audio/mpeg": ".mp3",
                        "video/mp4": ".mp4",
                        "video/x-m4v": ".m4v",
                        "audio/vnd.wave": '.wav'
                        }
        self.start = time.time()

    def selector(self):
        table = input("Table name?\n")
        collection_pid = input("Collection pid?\n")
        transform = input("Transform DC from MODS?\ny/n\n")
        if transform not in ['y', 'n']:
            print("Try again\n")
            self.selector()
        self.process_collection(table, collection_pid, transform)

    def get_foxml_from_pid(self, pid):
        foxml_file = self.ca.dereference(pid)
        foxml = f"{self.objectStore}/{foxml_file}"
        try:
            return FW.FWorker(foxml)
        except:
            print(f"No results found for {pid}")

    def process_collection(self, table, collection, transform_mods):
        collection_map = self.ca.get_collection_recursive_pid_model_map(table, collection)
        print(f"Processing {len(collection_map)} pids.")
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
            metadata = {}
            foxml = f"{self.objectStore}/{foxml_file}"
            try:
                fw = FW.FWorker(foxml)
            except:
                print(f"No record found for {pid}")
                continue
            dublin_core = None
            files_info = fw.get_file_data()
            if transform_mods == 'y' and 'MODS' in files_info:
                mods_path = f"{self.datastreamStore}/{self.ca.dereference(files_info['MODS']['filename'])}"
                metadata = self.apply_transform(mods_path, pid)

            if 'dublin_core' not in metadata:
                dublin_core = fw.get_modified_dc()
            all_files = fw.get_file_data()
            for entry, file_data in all_files.items():
                if entry in self.stream_map[model]:
                    filename = f"{pid.replace(':', '_')}_{entry}{self.mimemap[file_data['mimetype']]}"
                    copy_streams[
                        file_data[
                            'filename']] = filename
            path = f"{archive_path}/item_{item_number}"
            # Build directory
            Path(path).mkdir(parents=True, exist_ok=True)
            with open(f'{path}/dublin_core.xml', 'w') as f:
                f.write(metadata['dublin_core'])
            if 'thesis' in metadata:
                with open(f'{path}/metadata_thesis.xml', 'w') as f:
                    f.write(metadata['thesis'])
            if 'oaire' in metadata:
                with open(f'{path}/metadata_oaire.xml', 'w') as f:
                    f.write(metadata['oaire'])
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

    #  Function for NS Audio.  Metadata is drawn at collection level, Assets come from members.

    def nscad_audio(self, collection_pid, start_num):
        archive = collection_pid.replace(':', '_')
        archive_path = f"{self.export_dir}/nscad_4439"
        Path(archive_path).mkdir(parents=True, exist_ok=True)
        first_level = self.ca.get_subcollections('nscad', collection_pid)
        current_number = start_num
        for pid in first_level:
            fw = self.get_foxml_from_pid(pid)
            current_number += 1
            item_number = str(current_number).zfill(4)
            dublin_core = fw.get_modified_dc()
            files_info = fw.get_file_data()
            mods_path = f"{self.datastreamStore}/{self.ca.dereference(files_info['MODS']['filename'])}"
            metadata = self.apply_transform(mods_path, pid)
            copy_streams = {}
            second_level = self.ca.get_collection_pids('nscad', pid)
            for component in second_level:
                fworker = self.get_foxml_from_pid(component)
                file_data = fworker.get_file_data()
                if 'OBJ' in file_data:
                    copy_streams[
                        file_data['OBJ'][
                            'filename']] = f"{component.replace(':', '_')}_OBJ{self.mimemap[file_data['OBJ']['mimetype']]}"
            path = f"{archive_path}/item_{item_number}"
            # Build directory
            Path(path).mkdir(parents=True, exist_ok=True)
            with open(f'{path}/dublin_core.xml', 'w') as f:
                f.write(dublin_core)
            if 'thesis' in metadata:
                with open(f'{path}/metadata_thesis.xml', 'w') as f:
                    f.write(metadata['thesis'])
            if 'oaire' in metadata:
                with open(f'{path}/metadata_oaire.xml', 'w') as f:
                    f.write(metadata['oaire'])
            with open(f'{path}/contents', 'w') as f:
                for source, destination in copy_streams.items():
                    stream_to_copy = self.ca.dereference(source)
                    shutil.copy(f"{self.datastreamStore}/{stream_to_copy}", f"{path}/{destination}")
                    f.write(f"{destination}\n")
            print(f"item_{item_number}")
            return current_number

    def build_nscad_audio_collection(self, collection):
        subcollections = self.ca.get_subcollections('nscad', collection)
        end_num = self.nscad_audio(subcollections[0], 0)
        for subcollection in subcollections[1:]:
            end_num = self.nscad_audio(subcollection, end_num)


    def build_book(self, table, book_pid):
        archive = book_pid.replace(':', '_')
        archive_path = f"{self.export_dir}/{archive}"
        Path(archive_path).mkdir(parents=True, exist_ok=True)
        pages = self.ca.get_pages(table, book_pid)
        fw = self.get_foxml_from_pid(book_pid)
        files_info = fw.get_file_data()
        mods_path = f"{self.datastreamStore}/{self.ca.dereference(files_info['MODS']['filename'])}"
        metadata = self.apply_transform(mods_path, book_pid)
        dc = fw.get_modified_dc()
        path = f"{archive_path}/book_{book_pid.replace(':', '_')}"
        Path(path).mkdir(parents=True, exist_ok=True)
        for pid in pages:
            pfw = self.get_foxml_from_pid(pid)
            file_data = pfw.get_file_data()
            if 'JPEG' in file_data:
                source = f"{self.datastreamStore}/{file_data['JPEG']['filename']}"
                destination = f"{pid.replace(':', '_')}_OBJ_{self.mimemap[file_data['OBJ']['mimetype']]}"
                shutil.copy(source, f"{path}/{destination}")
        print(f"Zipping files into {archive}.zip")
        shutil.make_archive(f"{self.export_dir}/{archive}", 'zip', f"{self.export_dir}/{archive}")
        return {
            'dc': dc,
            'file': f"{self.export_dir}/{archive}.zip"
        }

    def get_nscc_ocr(self):
        collections = self.ca.get_subcollections('nscc', 'nscc:booktest')
        for collection in collections:
            fw = self.get_foxml_from_pid(collection)
            collection_title = fw.get_properties()['label'].strip().replace(" ", "_")
            collection_path = f"{self.export_dir}/{collection_title}"
            Path(collection_path).mkdir(parents=True, exist_ok=True)
            yearbooks = self.ca.get_books('nscc', collection)
            for yearbook in yearbooks:
                fw = self.get_foxml_from_pid(yearbook)
                yearbook_title = fw.get_properties()['label'].strip().replace(" ", "_")
                yearbook_path = f"{collection_path}/{yearbook_title}"
                Path(yearbook_path).mkdir(parents=True, exist_ok=True)
                pages = self.ca.get_pages('nscc', yearbook)
                print(f"Processing {len(pages)} pages for {yearbook_title}")
                for page in pages:
                    fw = self.get_foxml_from_pid(page)
                    file_data = fw.get_file_data()
                    rels = fw.get_rels_ext_values()
                    page_num = rels['isPageNumber']
                    if 'OCR' in file_data:
                        source = f"{self.datastreamStore}/{self.ca.dereference(file_data['OCR']['filename'])}"
                        destination = f"{yearbook_path}/{yearbook_title}_{page_num}{self.mimemap[file_data['OCR']['mimetype']]}"
                        try:
                            shutil.copy(source, destination)
                        except FileNotFoundError as e:
                            print(f"File not found for page: {page_num}")
                print(f"Zipping files into {yearbook_title}.zip")
                shutil.make_archive(yearbook_path, 'zip', yearbook_path)
                shutil.rmtree(yearbook_path)

    def save_all_datastreams(self, namespace, datastream):
        pids = self.ca.get_pids_from_objectstore(namespace)
        collection_path = f"{self.export_dir}/{namespace}_{datastream}"
        Path(collection_path).mkdir(parents=True, exist_ok=True)
        for pid in pids:
            fw = self.get_foxml_from_pid(pid)
            datastreams = fw.get_file_data()
            if datastream in datastreams:
                label = fw.get_properties()['label'].strip().replace(" ", "_")
                source = f"{self.datastreamStore}/{self.ca.dereference(datastreams[datastream]['filename'])}"
                destination = f"{collection_path}/{label}_{pid}_{datastream}{self.mimemap[datastreams[datastream]['mimetype']]}"
                try:
                    shutil.copy(source, destination)
                except FileNotFoundError as e:
                    print(f"File not found for: {pid}")
        print(f"Zipping files into {namespace}_{datastream}.zip")
        shutil.make_archive(collection_path, 'zip', collection_path)
        shutil.rmtree(collection_path)

    def apply_transform(self, mods_path, pid):
        return_files = {}
        dom = ET.parse(mods_path)
        xslt = ET.parse(self.mods_xsl)
        transform = ET.XSLT(xslt)
        dc = transform(dom)
        root = ET.Element("dublin_core")
        ET.SubElement(root, "dcvalue", element="identifier", qualifier="other").text = pid
        thesis_root = ET.Element("dublin_core")
        thesis_root.set('schema', 'thesis')
        oaire_root = ET.Element("dublin_core")
        oaire_root.set('schema', 'oaire')

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
            if tag == 'degree':
                ET.SubElement(thesis_root, "dcvalue", element=tag,
                              qualifier=qualifier).text = value.replace('%%%', ',')
            elif tag == 'citation':
                ET.SubElement(oaire_root, "dcvalue", element=tag,
                              qualifier=qualifier).text = value.replace('%%%', ',')
            else:
                ET.SubElement(root, "dcvalue", element=tag,
                              qualifier=qualifier).text = value.replace('%%%', ',')
        ET.indent(root, space="\t", level=0)
        return_files['dublin_core'] = ET.tostring(root, encoding='unicode')
        if len(thesis_root.xpath(".//*")) > 0:
            ET.indent(thesis_root, space="\t", level=0)
            return_files['thesis'] = ET.tostring(thesis_root, encoding='unicode')
        if len(oaire_root.xpath(".//*")) > 0:
            ET.indent(thesis_root, space="\t", level=0)
            return_files['oaire'] = ET.tostring(oaire_root, encoding='unicode')
        return return_files

    def batch_processor(self, table, collections):
        for collection in collections:
            self.process_collection(table, collection, 'y')


collections = ['umir:theses']
CP = CairnProcessor()
CP.build_nscad_audio_collection('nscad:4439')
