#!/usr/bin/env python3

import collections
import csv
import hashlib
import sqlite3
import urllib
import urllib.parse

import lxml.etree as ET
import requests


class CairnUtilities:
    def __init__(self):
        self.marcxml = 'assets/xsl/MODS3-4_MARC21slim_XSLT1-0.xsl'
        self.mods_xsl = 'assets/mods_to_dc.xsl'
        self.conn = sqlite3.connect('cairn.db')
        self.fields = ['PID', 'model', 'RELS_EXT_isMemberOfCollection_uri_ms', 'RELS_EXT_isPageOf_uri_ms']

    def mods_to_marc21(self, mods_xml):
        dom = ET.parse(mods_xml)
        xslt = ET.parse(self.marcxml)
        transform = ET.XSLT(xslt)
        newdom = transform(dom)
        return ET.tostring(newdom)

    def mods_to_dc(self, mods_xml):
        dom = ET.parse(mods_xml)
        xslt = ET.parse(self.mods_xsl)
        transform = ET.XSLT(xslt)
        newdom = transform(dom)
        return ET.tostring(newdom)

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

    def get_pids_from_csv(self, csv_file):
        print(csv_file)
        with open(csv_file, newline='') as csvfile:
            pids = []
            reader = csv.DictReader(csvfile)
            for row in reader:
                pid = row['PID']
                pids.append(pid)

    def process_institution(self, institution, csv_file):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE if not exists {institution}(
            pid TEXT PRIMARY KEY,
            content_model TEXT,
            collection_pid TEXT,
            page_of TEXT,
            sequence TEXT,
            constiuent_of TEXT
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

    def get_collection_pids(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT PID from {table} where collection_pid = '{collection}'"
        pids = []
        for row in cursor.execute(command):
            pids.append(row[0])
        return pids

    def get_collection_pid_model_map(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT PID, CONTENT_MODEL from {table} where collection_pid = '{collection}'"
        map = {}
        for row in cursor.execute(command):
            map[row[0]] = row[1]
        return map

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


if __name__ == '__main__':
    CA = CairnUtilities()
    # CA.process_institution('stfx', "inputs/stfx.csv")
    print(CA.dereference('nscc:9769+JP2+JP2.0'))
    print(CA.dereference('nscc:9769'))

    # CA.get_collection_pids('nscc', 'nscc:33056')
    # CA.get_stores('nscc:33037')
    # CA.get_all_mods()
    print(CA.get_collection_pid_model_map('nscc', 'nscc:33056'))
