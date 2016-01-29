import os
from xml.etree import ElementTree

class MetExtractorException(Exception):
    """Raises a MetExtractor exception."""
    pass

class MetExtractor(object):
    """Base class for handling metadata extraction. This class can be used to
    create an empty met file that complies with OODT ingest"

    Parameters
    ----------
    metadata_filename : string : name of the metadata output file to use.
        Filename for access handler to CAS metadata file.

    Attributes
    ----------
    metadata : dict : Dictionary containing metadata.
        A metadata dictionary containing file metadata.

    product_type : string : Specify product type for OODT Filemananger ingest
        Default value is 'GenericFile', the most basic OODT type.

    Hidden Attributes
    -----------------
    _metadata_extracted : boolean : keep track of metadata extraction.
        The access handler to a katfile.
    """

    def __init__(self, metadata_filename):
        super(MetExtractor, self).__init__()
        self.metadata_filename = metadata_filename
        self.metadata = {}
        self.product_type = None
        self._metadata_extracted = False

    def _extract_metadata_product_type(self):
        """Set the metadata product type. Raise an exception if it is not defined."""
        if self.product_type == None:
            raise MetExtractorException('No product type.')
        self.metadata['ProductTypeName'] = self.product_type

    def extract_metadata(self):
        """Extract the product type and ."""
        self._extract_metadata_product_type()
        self._metadata_extracted = True

    def __str__(self):
        """String representation as cas:metadata xml tree."""
        xml_tree = ElementTree.Element('cas:metadata')
        xml_tree.set('xmlns:cas', 'http://oodt.jpl.nasa.gov/1.0/cas')
        for k in self.metadata.keys():
            keyval = ElementTree.SubElement(xml_tree, 'keyval')
            key = ElementTree.SubElement(keyval, 'key')
            key.text = str(k)
            if isinstance(self.metadata[k], list):
                for text in self.metadata[k]:
                    val = ElementTree.SubElement(keyval, 'val')
                    val.text = text
            else:
                val = ElementTree.SubElement(keyval, 'val')
                val.text = self.metadata[k]
        return ElementTree.tostring(xml_tree, 'utf-8')

    def write_metadatafile(self):
        """Export metadata to file."""
        if self._metadata_extracted:
            with open(self.metadata_filename, 'w') as metfile:
                metfile.write(str(self))
        else:
            raise MetExtractorException('No metadata extracted.')

class ExampleMetExtractor(MetExtractor):
    """docstring for ExampleMetExtractor"""
    def __init__(self, filename, *args, **kwargs):
        super(ExampleMetExtractor, self).__init__(filename + '.met', *args, **kwargs)
        self.filename = os.path.abspath(filename)
        self.product_type = 'ExampleFileType'

    def _get_file_content(self):
        return (open(self.filename, 'r')).read()

    def extract_metadata(self):
        self.metadata['ProductName'] = self.filename
        self.metadata['FileContent'] = self._get_file_content()
        super(ExampleMetExtractor, self).extract_metadata()

if __name__ == "__main__":
    example_file = 'test.txt'
    with open(example_file, 'w') as f:
        lines = []
        lines.append('Line 1.\n')
        lines.append('Line 2.\n')
        lines.append('Line 3.\n')
        lines.append('Line 4.\n')
        f.writelines(lines)

    example = ExampleMetExtractor(filename=example_file)
    example.extract_metadata()
    example.write_metadatafile()