#!/usr/bin/env python3

import collections
import csv
import hashlib
import sqlite3
import urllib
import urllib.parse
from pathlib import Path
from urllib.parse import unquote

import lxml.etree as ET
import requests

import FoxmlWorker as FW


class CairnUtilities:
    def __init__(self):
        self.marcxml = 'assets/xsl/MODS3-4_MARC21slim_XSLT1-0.xsl'
        self.mods_xsl = 'assets/thesis.xsl'
        self.conn = sqlite3.connect('cairn.db')
        self.conn.row_factory = sqlite3.Row
        self.fields = ['PID', 'model', 'RELS_EXT_isMemberOfCollection_uri_ms', 'RELS_EXT_isPageOf_uri_ms']
        self.objectStore = '/usr/local/fedora/data/objectStore/'
        self.datastreamStore = '/usr/local/fedora/data/datastreamStore/'
        self.rels_map = {'isMemberOfCollection': 'collection_pid',
                         'isMemberOf': 'collection_pid',
                         'hasModel': 'content_model',
                         'isPageOf': 'page_of',
                         'isSequenceNumber': 'sequence',
                         'isConstituentOf': 'constituent_of'
                         }

    # Converts MODS to marc21
    def mods_to_marc21(self, mods_xml):
        dom = ET.parse(mods_xml)
        xslt = ET.parse(self.marcxml)
        transform = ET.XSLT(xslt)
        newdom = transform(dom)
        return ET.tostring(newdom)

    # Converts MODS to DC
    def mods_to_dc(self, mods_xml):
        dom = ET.parse(mods_xml)
        xslt = ET.parse(self.mods_xsl)
        transform = ET.XSLT(xslt)
        newdom = transform(dom)
        return ET.tostring(newdom)

    # Returns marc21 from PID - hardcoded for nscc
    def get_marc_from_pid(self, pid):
        url = f'https://nscc.cairnrepo.org/islandora/object/{pid}/datastream/MODS/download'
        mods_xml = requests.get(url).content
        dom = ET.fromstring(mods_xml)
        xslt = ET.parse(self.marcxml)
        transform = ET.XSLT(xslt)
        newdom = transform(dom)
        filename = f"MARC21/{pid.replace(':', '_')}.xml"
        with open(filename, 'wb') as f:
            newdom.write(f, encoding='utf-8')

    # Gets all pids from CSV file
    def get_pids_from_csv(self, csv_file):
        print(csv_file)
        with open(csv_file, newline='') as csvfile:
            pids = []
            reader = csv.DictReader(csvfile)
            for row in reader:
                pid = row['PID']
                pids.append(pid)

    # Creates database table with RELS-EXT values returned from Workbench harvest
    def process_institution(self, institution, csv_file):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE if not exists {institution}(
            pid TEXT PRIMARY KEY,
            content_model TEXT,
            collection_pid TEXT,
            page_of TEXT,
            sequence TEXT,
            constituent_of TEXT
            )""")
        self.conn.commit()
        with open(csv_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                collection = row['RELS_EXT_isMemberOfCollection_uri_ms'].replace("info:fedora/", '')
                page_of = row['RELS_EXT_isPageOf_uri_ms'].replace("info:fedora/", '')
                if not page_of:
                    page_of = ' '
                constituent_of = row['RELS_EXT_isConstituentOf_uri_ms'].replace("info:fedora/", '')
                if not page_of:
                    constituent_of = ' '
                try:
                    command = f"INSERT OR REPLACE INTO  {institution} VALUES('{row['PID']}', '{row['model']}', '{collection}','{page_of}', '{row['sequence']}','{constituent_of}')"
                    cursor.execute(command)
                except sqlite3.Error:
                    print(row['PID'])
        self.conn.commit()

    # Processes CSV returned from direct objectStore harvest
    def process_clean_institution(self, institution, csv_file):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE if not exists {institution}(
            pid TEXT PRIMARY KEY,
            content_model TEXT,
            collection_pid TEXT,
            page_of TEXT,
            sequence TEXT,
            constituent_of TEXT
            )""")
        self.conn.commit()
        with open(csv_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                collection = row['collection_pid']
                page_of = row['page_of']
                if not page_of:
                    page_of = ' '
                constituent_of = row['constituent_of']
                if not constituent_of:
                    constituent_of = ' '
                try:
                    command = f"INSERT OR REPLACE INTO  {institution} VALUES('{row['pid']}', '{row['content_model']}', '{collection}','{page_of}', '{row['sequence']}','{constituent_of}', '')"
                    cursor.execute(command)
                except sqlite3.Error:
                    print(row['pid'])
        self.conn.commit()

    # Identifies object and datastream location within Fedora objectStores and datastreamStore.
    def dereference(self, identifier: str) -> str:
        # Replace '+' with '/' in the identifier
        slashed = identifier.replace('+', '/')
        full = f"info:fedora/{slashed}"

        # Generate the MD5 hash of the full string
        hash_value = hashlib.md5(full.encode('utf-8')).hexdigest()

        # Pattern to fill with hash (similar to the `##` placeholder)
        subbed = "##"

        # Replace the '#' characters in `subbed` with the corresponding characters from `hash_value`
        hash_offset = 0
        pattern_offset = 0
        result = list(subbed)

        while pattern_offset < len(result) and hash_offset < len(hash_value):
            if result[pattern_offset] == '#':
                result[pattern_offset] = hash_value[hash_offset]
                hash_offset += 1
            pattern_offset += 1

        subbed = ''.join(result)
        # URL encode the full string, replacing '_' with '%5F'
        encoded = urllib.parse.quote(full, safe='').replace('_', '%5F')
        return f"{subbed}/{encoded}"

    # Get all collection pids within namespace
    def get_collection_pids(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT PID from {table} where collection_pid = '{collection}'"
        pids = []
        for row in cursor.execute(command):
            pids.append(row['pid'])
        return pids

    # Get all content models from map
    def get_collection_pid_model_map(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT PID, CONTENT_MODEL from {table} where collection_pid = '{collection}'"
        map = {}
        for row in cursor.execute(command):
            map[row[0]] = row[1]
        return map

    def get_subcollections(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT PID, CONTENT_MODEL from {table} where collection_pid = '{collection}' AND CONTENT_MODEL = 'islandora:collectionCModel' "
        pids = []
        for row in cursor.execute(command):
            pids.append(row[0])
        return pids

    def get_pages(self, table, book_pid):
        cursor = self.conn.cursor()
        command = f"SELECT PID from {table} where page_of = '{book_pid}'"
        pids = []
        for row in cursor.execute(command):
            pids.append(row[0])
        return pids
    # Gets collection hierarchy by namespace.
    def get_collection_details(self, table):
        cursor = self.conn.cursor()
        command = f"SELECT PID, COLLECTION_PID from {table} where content_model = 'islandora:collectionCModel'"
        results = {}
        for row in cursor.execute(command):
            results[row[0]] = row[1]
        return results

    # Gets filestores for NSCC - No longer needed.
    def get_stores(self, collection_pid):
        members = self.get_collection_pids('nscc', collection_pid)
        encoded_members = []
        for member in members:
            encoded_members.append(urllib.parse.quote(member))
        files = {'object': 'inputs/nscc_object.txt', 'datastream': 'inputs/nscc_datastream.txt'}
        for type, filename in files.items():
            file = open(filename)
            filtered_lines = []
            while True:
                line = file.readline()
                if any(x in line for x in encoded_members):
                    filtered_lines.append(line)
                if not line:
                    with open(f'fedora_stores/{collection_pid.replace(":", "_")}_{type}', 'w') as f:
                        for line in filtered_lines:
                            f.write(line)
                    break

    # Get MODS from NSCC inpupts
    def get_all_mods(self):
        all_mods = {}
        file = open('inputs/nscc_datastream.txt')
        while True:
            line = file.readline()
            if 'MODS' in line:
                parts = line.split('%2F')
                pid = parts[1]
                version = line[-1]
                if pid in all_mods:
                    last_version = all_mods[pid][-1]
                    if version > last_version:
                        all_mods[pid] = line
                else:
                    all_mods[pid] = line
            if not line:
                od = collections.OrderedDict(sorted(all_mods.items()))
                with open(f'assets/nscc_mods', 'w') as f:
                    for pid, line in od.items():
                        f.write(line)
                break

    # Gets PIDS, filtered by namespace directly from objectStore
    def get_pids_from_objectstore(self, namespace=''):
        wildcard = '*/*'
        if namespace:
            wildcard = f'*/*{namespace}*'
        pids = []
        for p in Path(self.objectStore).rglob(wildcard):
            pid = unquote(p.name).replace('info:fedora/', '')
            pids.append(pid)
        return pids

    # Gets all namespaces in objectStore
    def get_namespaces(self):
        namespaces = []
        for pid in self.get_pids_from_objectstore():
            namespace = pid.split(':')[0]
            if namespace not in namespaces:
                namespaces.append(namespace)
        return namespaces

    # Gets RELS-EXT relationships from objectStore
    def build_record_from_pids(self, namespace, output_file):
        pids = self.get_pids_from_objectstore(namespace)
        headers = ['pid',
                   'content_model',
                   'collection_pid',
                   'page_of',
                   'sequence',
                   'constituent_of']

        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for pid in pids:
                foxml_file = self.dereference(pid)
                foxml = f"{self.objectStore}/{foxml_file}"
                fw = FW.FWorker(foxml)
                if fw.get_state() != 'Active':
                    continue
                relations = fw.get_rels_ext_values()
                row = {}
                row['pid'] = pid
                for relation, value in relations.items():
                    if relation in self.rels_map:
                        row[self.rels_map[relation]] = value
                writer.writerow(row)

    # Adds all MODS records from datastreamStore to database
    def add_mods_to_database(self, namespace):
        cursor = self.conn.cursor()
        pids = self.get_pids_from_objectstore(namespace)
        for pid in pids:
            foxml_file = self.dereference(pid)
            foxml = f"{self.objectStore}/{foxml_file}"
            fw = FW.FWorker(foxml)
            if fw.get_state() != 'Active':
                continue
            mapping = fw.get_file_data()
            mods_info = mapping.get('MODS')
            if mods_info:
                mods_path = f"{self.datastreamStore}/{self.dereference(mods_info['filename'])}"
                mods_xml = Path(mods_path).read_text()
                if mods_xml:
                    mods_xml = mods_xml.replace("'", "''")
                    command = f"""UPDATE {namespace} set mods = '{mods_xml}' where pid = '{pid}"""
                    cursor.execute(command)
        self.conn.commit()



if __name__ == '__main__':
    CA = CairnUtilities()
    CA.build_record_from_pids('mta', 'mta.csv')

