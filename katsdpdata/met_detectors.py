import os
import re

import katsdptelstate

from katsdpdata.meerkat_product_extractors import MeerKATTelescopeProductMetExtractor, MeerKATFlagProductMetExtractor

STREAM_TYPES= {'MeerKATTelescopeProduct' : '^[0-9]{10}[-_]sdp[-_](l0$|l0[-_]continuum$)',
               'MeerKATFlagProduct' : '^[0-9]{10}[-_]sdp[-_](l1[-_]flags$|l1[-_]flags[-_]continuum$)'}

class ProductTypeDetectionError(Exception):
    pass

def file_type_detection(filename):
    """Detect product type by using the file extension. This curerntly only supports
    detection of.rdb files but using telstate to inspect the .rdb to see what the
    stream name is set to. This should be either 'sdp.vis' or 'sdp.flag'

    Parameters
    ----------
    filename: string : full path to the product

    Returns
    -------
    MetExtractor: object: A metadata extractor object to extract metadata from the product
    """
    ext = os.path.splitext(filename)[1]
    if ext == '.rdb':
        return telstate_detection(filename)
    raise ProductTypeDetectionError('%s from %s not a valid file type.' % (ext, filename))

def stream_type_detection(stream_name):
    """Given a stream name we need to detect they typ for creating metadata for stream products.
    Uses STREAM_TYPES to detect. Supports products of the format:
        (1) 1234567890-sdp-0 == MeerKATTelescopeProduct
        (2) 1234567890-sdp-0-continuum == MeerKATTelescopeProduct
        (3) 1234567890-sdp-1-flags == MeerKATFlagProduct
        (4) 1234567890-sdp-1-flags-continumm == MeerKATFlagProduct

    Parameters
    ----------
    stream_name: string : the name of the stream to detect.
    """
    stream_type = None
    for s_key in STREAM_TYPES.keys():
        if re.match(STREAM_TYPES[s_key], stream_name):
            stream_type = s_key
            break
    return stream_type

def telstate_detection(filename):
    """File is an .rdb files. Any .rdb files passed are assumed to be
    MeerKAT telescope products, and should be either an 'sdp.vis' or
    'sdp.flags' stream.

    Parameters
    ----------
    filename: string : full path to the product

    Returns
    -------
    MetExtractor: class : A metadata extractor class to extract metadata from the rdb file.
    """
    ts = katsdptelstate.TelescopeState()
    ts.load_from_file(filename)
    stream_name = ts['stream_name']
    v = ts.view(stream_name)
    if v['stream_type'] == 'sdp.vis':
        return MeerKATTelescopeProductMetExtractor(filename)
    elif v['stream_type'] == 'sdp.flags':
        return MeerKATFlagProductMetExtractor(filename)
    raise ProductTypeDetectionError('%s not a recognisable stream type')
