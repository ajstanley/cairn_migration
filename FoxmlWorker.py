import lxml.etree as ET


class FWorker:
    def __init__(self, foxml_file):
        self.tree = ET.parse(foxml_file)
        self.root = self.tree.getroot()
        self.dc = ["format", "language", "relation", "description", "coverage", "identifier",
                   "subject", "contributor", "publisher", "date", "title", "rights", "type",
                   "source", "creator"]

    # Returns PID from foxml
    def get_pid(self):
        return self.root.attrib['PID']

    # gets state from foxml
    def get_state(self):
        ns = {'': 'info:fedora/fedora-system:def/foxml#'}
        return self.root.find('.//objectProperties/property', ns).attrib['VALUE']

    # Gets all datastream types from foxml.
    def get_datastreams(self):
        ns = {'': 'info:fedora/fedora-system:def/foxml#'}
        datastreams = self.root.findall('.//datastream', ns)
        types = {}
        for datastream in datastreams:
            versions = datastream.findall('./datastreamVersion', ns)
            mimetype = versions[-1].attrib['MIMETYPE']
            types[datastream.attrib['ID']] = mimetype
        return types

    # Gets names of current managed files from foxml.
    def get_file_data(self):
        mapping = {}
        streams = self.get_datastreams()
        ns = {'foxml': 'info:fedora/fedora-system:def/foxml#'}
        for stream, mimetype in streams.items():
            location = self.root.xpath(
                f'//foxml:datastream[@ID="{stream}"]/foxml:datastreamVersion/foxml:contentLocation', namespaces=ns)
            if location:
                mapping[stream] = {'file_name': location[-1].attrib['REF'], 'mimetype': mimetype}
        return mapping

    # Converts embedded dublin core to dspace dublin core
    def get_modified_dc(self):
        ns = {'foxml': 'info:fedora/fedora-system:def/foxml#'}
        namespaces = {
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
            'dc': 'http://purl.org/dc/elements/1.1/'
        }
        dc_nodes = self.root.findall(f'.//foxml:datastream[@ID="DC"]/foxml:datastreamVersion/foxml:xmlContent',
                                     namespaces=ns)
        dc_node = dc_nodes[-1]
        dc_values = {}
        for tag in self.dc:
            test = dc_node.find(f".//dc:{tag}", namespaces)
            if test is not None:
                dc_values[tag] = test.text
        dspace_dc = self.build_dc(dc_values)
        return dspace_dc

    # Builds dspace xml from extracted values/
    def build_dc(self, dc_values):
        root = ET.Element("dublin_core")
        for key, value in dc_values.items():
            value = value.replace("\\,", '%%%')
            values = value.split(',')
            for x in values:
                ET.SubElement(root, "dcvalue", element=key,
                              qualifier='none').text = x.replace('%%%', ',')
            ET.indent(root, space="\t", level=0)
        return ET.ElementTree(root)


FW = FWorker('inputs/sample_foxml.xml')
datastreams = FW.get_file_data()
print(datastreams)

