import os
import katsdptelstate
from .met_extractors import (
    MeerKATTelescopeProductMetExtractor,
    MeerKATFlagProductMetExtractor,
)


class ProductTypeDetectionError(Exception):
    pass


def file_type_detection(filename):
    """Detect product type by using the file extension.

    Parameters
    ----------
    filename: string : full path to the product

    Returns
    -------
    MetExtractor: class : A metadata extractor class to extract metadata from the product
    """
    ext = os.path.splitext(filename)[1]
    if ext == ".rdb":
        return telstate_detection(filename)
    raise ProductTypeDetectionError(
        "%s from %s not a valid file type." % (ext, filename)
    )


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
    stream_name = ts["stream_name"]
    v = ts.view(stream_name)
    if v["stream_type"] == "sdp.vis":
        return MeerKATTelescopeProductMetExtractor
    elif v["stream_type"] == "sdp.flags":
        return MeerKATFlagProductMetExtractor
    raise ProductTypeDetectionError("%s not a recognisable stream type")
