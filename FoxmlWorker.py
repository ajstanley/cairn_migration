from pathlib import Path

import lxml.etree as ET


class FWorker:
    def __init__(self, foxml_file):
        self.tree = ET.parse(foxml_file)
        self.root = self.tree.getroot()
        self.namespaces = {
            'foxml': 'info:fedora/fedora-system:def/foxml#',
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
            'dc': 'http://purl.org/dc/elements/1.1/'
        }
        self.mods_xsl = 'assets/mods_to_dc.xsl'
        self.properties = self.get_properties()

    # Returns PID from foxml
    def get_pid(self):
        return self.root.attrib['PID']

    # gets state
    def get_state(self):
        return self.properties['state']

    def get_properties(self):
        values = {}
        properties = self.root.findall('.//foxml:objectProperties/foxml:property', self.namespaces)
        for property in properties:
            name = property.attrib['NAME'].split('#')[1]
            value = property.attrib['VALUE']
            values[name] = value
        return values

    # Gets all datastream types from foxml.
    def get_datastreams(self):
        ns = {'': 'info:fedora/fedora-system:def/foxml#'}
        datastreams = self.root.findall('.//foxml:datastream', self.namespaces)
        types = {}
        for datastream in datastreams:
            versions = datastream.findall('./foxml:datastreamVersion', self.namespaces)
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

    def get_dc_values(self):
        dc_nodes = self.root.findall(f'.//foxml:datastream[@ID="DC"]/foxml:datastreamVersion/foxml:xmlContent',
                                     namespaces=self.namespaces)
        dc_values = []
        dc_node = dc_nodes[-1]
        for child in dc_node.iter():
            if child.text is not None:
                cleaned = child.text.replace('\n', '')
                text = ' '.join(cleaned.split())
                if text:
                    tag = child.xpath('local-name()')
                    dc_values.append({tag: text})
        return dc_values

    # Converts embedded dublin core to dspace dublin core
    def get_modified_dc(self):
        dc_nodes = self.root.findall(f'.//foxml:datastream[@ID="DC"]/foxml:datastreamVersion/foxml:xmlContent',
                                     namespaces=self.namespaces)
        dc_node = dc_nodes[-1]
        return self.build_dspace_dc(dc_node)

    # Builds dspace xml from extracted values/
    def build_dspace_dc(self, dc_node):
        root = ET.Element("dublin_core")
        dc_values = self.get_dc_values()
        for candidate in dc_values:
            for key, value in candidate.items():
                value = value.replace("\\,", '%%%')
                ET.SubElement(root, "dcvalue", element=key,
                              qualifier='none').text = value.replace('%%%', ',')

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
    print(FW.get_dc_values())
    print(FW.properties['label'])
    print(FW.properties['state'])

    # dc = FW.get_modified_dc()
    # values = FW.get_dc_values()
    # print(values)
