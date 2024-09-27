import csv
import xml.etree.cElementTree as ET
import os
from pathlib import Path
import CairnUtilities as CA


class ArchiveBuilder:

    def __init__(self, input, archive_dir):
        self.input = input
        self.dc = ["format", "language", "relation", "description", "coverage", "identifier",
                   "subject", "contributor", "publisher", "date", "title", "rights", "type",
                   "source", "creator"]
        self.archive_dir = f"{os.curdir}/archives/{archive_dir}"
        Path(self.archive_dir).mkdir(parents=True, exist_ok=True)

    def work(self):
        with open(self.input, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                item_number = row['ID'].zfill(3)
                dir_item = f"item_{item_number}"
                path = f"{self.archive_dir}/{dir_item}"
                # Build directory
                Path(path).mkdir(parents=True, exist_ok=True)
                # Build and save Dublin core
                dublin_core = self.build_dc(row)
                with open(f'{path}/dublin_core.xml', 'w') as f:
                    dublin_core.write(f, encoding='unicode')


    def build_dc(self, row):
        root = ET.Element("dublin_core")
        for identifier in self.dc:
            value = row[f"dc.{identifier}"].replace("\\,", '%%%')
            values = value.split(',')
            for x in values:
                ET.SubElement(root, "dcvalue", element=identifier,
                              qualifier='none').text = x.replace('%%%', ',')
        ET.indent(root, space="\t", level=0)
        return ET.ElementTree(root)

if __name__ == '__main__':
    AB = ArchiveBuilder("inputs/msvu.csv", 'msvu_archive')
    AB.work()

