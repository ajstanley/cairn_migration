from pathlib import Path

import lxml.etree as ET


class FWorker:
    def __init__(self, foxml_file):
        self.tree = ET.parse(foxml_file)
        self.root = self.tree.getroot()
        self.dc = ["format", "language", "relation", "description", "coverage", "identifier",
                   "subject", "contributor", "publisher", "date", "title", "rights", "type",
                   "source", "creator"]
        self.namespaces = {
            'foxml': 'info:fedora/fedora-system:def/foxml#',
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
            'dc': 'http://purl.org/dc/elements/1.1/'
        }
        self.mods_xsl = 'assets/mods_to_dc.xsl'

    # Returns PID from foxml
    def get_pid(self):
        return self.root.attrib['PID']

    # gets state from foxml
    def get_state(self):
        return self.root.find('.//foxml:objectProperties/foxml:property', self.namespaces).attrib['VALUE']

    # Gets all datastream types from foxml.
    def get_datastreams(self):
        ns = {'': 'info:fedora/fedora-system:def/foxml#'}
        datastreams = self.root.findall('.//foxml:datastream', self.namespaces)
        types = {}
        for datastream in datastreams:
            versions = datastream.findall('./foxml:datastreamVersion', ns)
            mimetype = versions[-1].attrib['MIMETYPE']
            types[datastream.attrib['ID']] = mimetype
        return types

    # Gets names of current managed files from foxml.
    def get_file_data(self):
        mapping = {}
        streams = self.get_datastreams()
        for stream, mimetype in streams.items():
            location = self.root.xpath(
                f'//foxml:datastream[@ID="{stream}"]/foxml:datastreamVersion/foxml:contentLocation',
                namespaces=self.namespaces)
            if location:
                mapping[stream] = {'file_name': location[-1].attrib['REF'], 'mimetype': mimetype}
        return mapping

    def get_dc(self):
        dc_nodes = self.root.findall(
            f'.//foxml:datastream[@ID="DC"]/foxml:datastreamVersion/foxml:xmlContent/oai_dc:dc',
            namespaces=self.namespaces)
        dc_node = dc_nodes[-1]
        return ET.tostring(dc_node, encoding='unicode')

    # Converts embedded dublin core to dspace dublin core
    def get_modified_dc(self):
        dc_nodes = self.root.findall(f'.//foxml:datastream[@ID="DC"]/foxml:datastreamVersion/foxml:xmlContent',
                                     namespaces=self.namespaces)
        dc_node = dc_nodes[-1]
        return self.build_dspace_dc(dc_node)

    # Builds dspace xml from extracted values/
    def build_dspace_dc(self, dc_node):
        dc_values = {}
        for tag in self.dc:
            test = dc_node.find(f".//dc:{tag}", self.namespaces)
            if test is not None:
                dc_values[tag] = test.text
        root = ET.Element("dublin_core")
        for key, value in dc_values.items():
            value = value.replace("\\,", '%%%')
            values = value.split(',')
            for x in values:
                ET.SubElement(root, "dcvalue", element=key,
                              qualifier='none').text = x.replace('%%%', ',')
            ET.indent(root, space="\t", level=0)
        return ET.tostring(root, encoding='unicode')

    def transform_mods_to_dc(self):
        mods_xml = self.get_mods()
        dom = ET.parse(mods_xml)
        xslt = ET.parse(self.mods_xsl)
        transform = ET.XSLT(xslt)
        dc_node = transform(dom)
        return self.build_dspace_dc(dc_node)

    def get_mods(self):
        mappings = self.get_file_data()
        mod_path = mappings.get('MODS')
        if mod_path:
            return Path(mod_path).read_text()


if __name__ == '__main__':
    FW = FWorker('inputs/sample_foxml.xml')
    dc = FW.get_modified_dc()
    print(dc)
