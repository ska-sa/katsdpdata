import os
import subprocess
import time

import xml.etree.ElementTree as ET

class MetExtractorException(object):
    """Raises a MetExtractor exception."""
    pass

class MetExtractor(object):
    """Base class for handling metadata extraction. This class can be used to
    create an empty met file that complies with OODT ingest"

    Attributes
    ----------
    metadata : dict
        Holds key-value pairs for metadata.
    """
    def __init__(self):
        super(MetExtractor, self).__init__()
        self.metadata = {}

    def set_metadata(self):
        raise NotImplementedError

    def get_metadata(self):
        xml_tree = ET.Element('cas:metadata')
        xml_tree.set('xmlns:cas', 'http://oodt.jpl.nasa.gov/1.0/cas')
        for k in self.metadata.keys():
            keyval = ET.SubElement(xml_tree, 'keyval')
            key = ET.SubElement(keyval, 'key')
            key.text = str(k)
            if isinstance(self.metadata[k], list):
                for text in self.metadata[k]:
                    val = ET.SubElement(keyval, 'val')
                    val.text = text
            else:
                val = ET.SubElement(keyval, 'val')
                val.text = self.metadata[k]
        return ET.tostring(xml_tree, 'utf-8')

class KAT7MetExtractor(MetExtractor):
    """Used for extracting metdata from an HDF5 KAT File.

    Parameters
    ----------
    katdata : class : DataSet
        The access handler to a HDF5 katdata.

    Attributes
    ----------
    katdata : class : DataSet
        The access handler to a HDF5 katdata.
    """
    def __init__(self, katdata):
        super(KAT7MetExtractor, self).__init__()
        self.katdata = katdata
        self.metadata['ProductType'] = 'KatFile'

    def set_metadata(self):
        """Populate self.metadata with information scraped from self.katdata"""
        self.metadata['Antennas'] = [a.name for a in self.katdata.ants]
        self.metadata['CenterFrequency'] = str(self.katdata.channel_freqs[self.katdata.channels[-1]/2])
        self.metadata['ChannelWidth'] = str(self.katdata.channel_width)
        self.metadata['Description'] = self.katdata.description
        self.metadata['Details'] = str(self.katdata)
        self.metadata['DumpPeriod'] = '%.4f' % (self.katdata.dump_period)
        self.metadata['Duration'] = str(round(self.katdata.end_time-self.katdata.start_time, 2))
        self.metadata['ExperimentID'] = self.katdata.experiment_id
        self.metadata['FileSize'] = str(os.path.getsize(self.katdata.file.filename))
        self.metadata['InstructionSet'] = '%s %s' % (self.katdata.obs_params['script_name'], self.katdata.obs_params['script_arguments'])
        self.metadata['KatfileVersion'] = self.katdata.version
        self.metadata['KatpointTargets'] = list(set([str(i).replace('tags=','') for i in self.katdata.catalogue.targets if i.name not in ['None', 'Nothing']]))
        self.metadata['NumFreqChannels'] = str(len(self.katdata.channels))
        self.metadata['Observer'] = self.katdata.observer
        self.metadata['RefAntenna'] = self.katdata.ref_ant
        self.metadata['StartTime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(self.katdata.start_time))
        self.metadata['Targets'] = list(set([i.name for i in self.katdata.catalogue.targets if i.name not in ['None', 'Nothing', 'azel', 'radec']]))

        print 'Calculating the md5 checksum for %s. This may take a while.' % (self.katdata.file.filename)
        p = subprocess.Popen(['md5sum', self.katdata.file.filename], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if not p[1]:
            self.metadata['FileDigest'] = p[0].split()[0]
        print 'md5 checksum complete. Digest is %s.' % self.metadata['FileDigest']

class RTSMetExtractor(KAT7MetExtractor):
    """Used for extracting metdata from an HDF5 KAT File that has been captured by the RTS.
    Parameters
    ----------
    katdata : class : DataSet
        The access handler to a HDF5 katdata.

    Attributes
    ----------
    katdata : class : DataSet
        The access handler to a HDF5 katdata.
    """
    def __init__(self, katdata):
        super(RTSMetExtractor, self).__init__(katdata)
        self.metadata['ProductType'] = 'RTSTelescopeProduct'

    def set_metadata(self):
        """Populate self.metadata with information scraped from self.katdata"""
        self.metadata['ReductionName'] = self.katdata.obs_params.get('reduction_name', '')
        super(RTSMetExtractor, self).set_metadata()

class KATContPipeExtractor(MetExtractor):
    """Used for extracting metdata from a KAT Cont Pipe VOTable xml file.

    Parameters
    ----------
    votable : ElementTree
        An xml string

    Attributes
    ----------
    """
    def __init__(self, project):
        super(KAT7MetExtractor, self).__init__()
        self.votable = votable
        self.metadata['ProductType'] = 'KATContPipeReductionProduct'

    def set_metadata(self):
        """Populate self.metadata with information scraped from a votable xml file"""
        self.metadata['aipsVer'] = project['aipsVer']
        self.metadata['AmpCals'] = project['AmpCals'].split()
        self.metadata['anNames'] = project['anNames'].split()
        self.metadata['archFileID'] = project['archFileID']
        self.metadata['BPCals'] = project['BPCals'].split()
        self.metadata['dataSet'] = project['dataSet']
        self.metadata['DlyCals'] = project['DlyCals'].split()
        self.metadata['fileSetID'] = project['fileSetID']
        self.metadata['freqCov'] = project[float(f) for f in project['freqCov'].split()]
        self.metadata['minFringe'] = float(project['minFringe'])
        self.metadata['obitVer'] = project['obitVer']
        self.metadata['PhsCals'] = project['PhsCals'].split()
        self.metadata['pipeVer'] = project['pipeVer']
        self.metadata['procDate'] = project['procDate']
        self.metadata['project'] = project['project']
        self.metadata['pyVer'] = project['pyVer']
        self.metadata['session'] = project['sessioin']
        self.metadata['sysInfo'] = project['sysInfo']

