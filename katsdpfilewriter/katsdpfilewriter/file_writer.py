"""
Writes L0 data plus metadata to an HDF5 file.
"""

import os
import logging
import h5py
import numpy as np

from katdal.h5datav3 import FLAG_NAMES


# the version number is intrinsically linked to the telescope model, as this
# is the arbiter of file structure and format
HDF5_VERSION = "3.0"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# TODO: share these with katdal
_TIMESTAMPS_DATASET = '/Data/timestamps'
_FLAGS_DATASET = '/Data/flags'
_FLAGS_DESCRIPTION_DATASET = '/Data/flags_description'
_CBF_DATA_DATASET = '/Data/correlator_data'
_WEIGHTS_DATASET = '/Data/weights'
_WEIGHTS_CHANNEL_DATASET = '/Data/weights_channel'
_TSTATE_DATASET = '/TelescopeState'


def _split_array(array, dtype):
    """Return a view of `array` which has one extra dimension. Each element is `array` is
    treated as some number of elements of type `dtype`, whose size must divide
    into the element size of `array`."""
    in_dtype = array.dtype
    out_dtype = np.dtype(dtype)
    if in_dtype.hasobject or out_dtype.hasobject:
        raise ValueError('dtypes containing objects are not supported')
    if in_dtype.itemsize % out_dtype.itemsize != 0:
        raise ValueError('item size does not evenly divide')

    interface = dict(array.__array_interface__)
    if interface.get('mask', None) is not None:
        raise ValueError('masked arrays are not supported')
    interface['shape'] = array.shape + (in_dtype.itemsize // out_dtype.itemsize,)
    if interface['strides'] is not None:
        interface['strides'] = array.strides + (out_dtype.itemsize,)
    interface['typestr'] = out_dtype.str
    interface['descr'] = out_dtype.descr
    return np.asarray(np.lib.stride_tricks.DummyArray(interface, base=array))


def _array_encode(value):
    """Convert array of Unicode values to UTF-8 encoding for storage in HDF5"""
    if isinstance(value, bytes) or isinstance(value, unicode):
        # h5py has special handling for these: see h5py._hl.base.guess_dtype.
        return value
    value = np.asarray(value)
    if value.dtype.kind == 'U':
        return np.core.defchararray.encode(value, 'utf-8')
    else:
        return value


def set_telescope_model(h5_file, model_data, base_path="/TelescopeModel"):
    """Sets the tree of telescope model data on an HDF5 file."""
    for component in model_data.components.values():
        comp_base = "{0}/{1}/".format(base_path, component.name)
        try:
            c_group = h5_file.create_group(comp_base)
            c_group.attrs['class'] = str(component.__class__.__name__)
        except ValueError:
            c_group = h5_file[comp_base]
            logger.warning("Failed to create group %s (likely to already exist)", comp_base)
        for attribute in component.attributes:
            try:
                value = model_data.get_attribute_value(attribute)
                if value is not None:
                    c_group.attrs[attribute.name] = _array_encode(value)
            except Exception:
                logger.warning("Exception thrown while storing attribute %s", attribute.name, exc_info=True)
        for sensor in sorted(component.sensors, key=lambda sensor: sensor.name):
            try:
                data = model_data.get_sensor_values(sensor)
                if data is not None:
                    try:
                        dset = np.rec.fromrecords(data, names='timestamp, value, status')
                        dset.sort(axis=0)
                        c_group.create_dataset(sensor.name, data=dset)
                        if sensor.description is not None:
                            c_group[sensor.name].attrs['description'] = sensor.description
                    except IndexError:
                        logger.warning("Failed to create dataset %s/%s as the model has no values",
                                       comp_base, sensor.name)
                    except RuntimeError:
                        logger.warning("Failed to insert dataset %s/%s as it already exists",
                                       comp_base, sensor.name)
            except Exception:
                logger.warning("Exception thrown while storing sensor %s", sensor.name, exc_info=True)


def set_telescope_state(h5_file, tstate, base_path=_TSTATE_DATASET, start_timestamp=0):
    """Write raw pickled telescope state to an HDF5 file."""
    tstate_group = h5_file.create_group(base_path)
    tstate_group.attrs['subarray_product_id'] = tstate.get('subarray_product_id','none')
     # include the subarray product id for use by the crawler to identify which system the file belongs to
    tstate_keys = tstate.keys()
    logger.info("Writing {} telescope state keys to {}".format(len(tstate_keys), base_path))

    for key in tstate_keys:
        if not tstate.is_immutable(key):
            sensor_values = tstate.get_range(key, st=start_timestamp,
                                             include_previous=True, return_pickle=True)
             # retrieve all values for a particular key
            # swap value, timestamp to timestamp, value
            sensor_values = [(timestamp, value) for (value, timestamp) in sensor_values]
            dset = np.rec.fromrecords(sensor_values, names='timestamp,value')
            tstate_group.create_dataset(key, data=dset)
            logger.debug("TelescopeState: Written {} values for key {} to file".format(len(dset), key))
        else:
            tstate_group.attrs[key] = tstate.get(key, return_pickle=True)
            logger.debug("TelescopeState: Key {} written as an attribute".format(key))


class File(object):
    def __init__(self, filename, stream_name=None):
        """Initialises an HDF5 output file as appropriate for this version of
        the telescope model."""
        # Need to use at least version 1.8, so that >64K attributes
        # (specifically bls_ordering) can be stored. At present there is no
        # way to explicitly request 1.8; this should be revisited after 1.10
        # ships.
        h5_file = h5py.File(filename, mode="w", libver='latest')
        data_group = h5_file['/'].create_group('Data')
        if stream_name is not None:
            data_group.attrs['stream_name'] = stream_name
        h5_file['/'].attrs['version'] = HDF5_VERSION
        self._h5_file = h5_file

    def set_timestamps(self, timestamps):
        """Write all timestamps for the file in one go. This must only be
        called once, as it creates the dataset.
        """
        ds = self._h5_file.create_dataset(_TIMESTAMPS_DATASET, data=timestamps)
        ds.attrs['timestamp_reference'] = 'centroid'
        self._h5_file.flush()

    def create_data(self, shape):
        """Creates the data sets for visibilities, weights and flags."""
        shape = list(shape)  # Ensures that + works below
        chunk_channels = min(32, shape[0])
        self._h5_file.create_dataset(
                _CBF_DATA_DATASET, [0] + shape + [2],
                maxshape=[None] + shape + [2], dtype=np.float32,
                chunks=(1, chunk_channels, shape[1], 2))
        self._h5_file.create_dataset(
                _FLAGS_DATASET, [0] + shape,
                maxshape=[None] + shape, dtype=np.uint8,
                chunks=(1, chunk_channels, shape[1]),
                fillvalue=np.uint8(2**FLAG_NAMES.index('data_lost')))
        self._h5_file.create_dataset(
                _WEIGHTS_DATASET, [0] + shape,
                maxshape=[None] + shape, dtype=np.uint8,
                chunks=(1, chunk_channels, shape[1]),
                fillvalue=np.uint8(1))
        self._h5_file.create_dataset(
                _WEIGHTS_CHANNEL_DATASET, [0] + shape[:1],
                maxshape=[None] + shape[:1], dtype=np.float32,
                chunks=(1, shape[0]),
                fillvalue=np.float32(1))

    def add_data_heap(self, vis, flags, weights, weights_channel, time_idx, channel0):
        """Add a single visibility/flags heap to the file (which may contain
        only a subinterval of the channels). The datasets must have already
        been created by :meth:`create_data`.

        Parameters
        ----------
        vis : numpy array, complex64, dimensions channels and baselines
            Visibilities
        flags : numpy array, uint8, dimensions channels and baselines
            Flags
        weights : numpy array, uint8, dimensions channels and baselines
            Detailed weights, which must be scaled by `weights_channel`
            to get the actual weights
        weights_channel : numpy array, float32, dimensions channels
            Coarse weights
        time_idx : int
            File position along the time axis
        channel0 : int
            Offset of first channel in the data arrays
        """
        # resize datasets
        h5_cbf = self._h5_file[_CBF_DATA_DATASET]
        h5_flags = self._h5_file[_FLAGS_DATASET]
        h5_weights = self._h5_file[_WEIGHTS_DATASET]
        h5_weights_channel = self._h5_file[_WEIGHTS_CHANNEL_DATASET]
        if h5_cbf.shape[0] <= time_idx:
            h5_cbf.resize(time_idx+1, axis=0)
            h5_flags.resize(time_idx+1, axis=0)
            h5_weights.resize(time_idx+1, axis=0)
            h5_weights_channel.resize(time_idx+1, axis=0)
        channel_slice = np.s_[channel0 : channel0 + vis.shape[0]]

        # Complex values are written to file as an extra dimension of size 2,
        # rather than as structs. Revisit this later to see if either the HDF5
        # file format can be changed to store complex data (rather than
        # having a real/imag axis for reals).
        vis_pairs = _split_array(vis, np.float32)
        h5_cbf[time_idx, channel_slice] = vis_pairs
        h5_flags[time_idx, channel_slice] = flags
        h5_weights[time_idx, channel_slice] = weights
        h5_weights_channel[time_idx, channel_slice] = weights_channel
        self._h5_file.flush()

    def set_metadata(self, model_data, base_path="/TelescopeModel"):
        """
        Writes to the telescope model group of the HDF5 file.

        Parameters:
        -----------
        model_data : :class:`telescope_model.TelescopeModelData`
            The telescope model with a view of the data
        base_path : str, optional
            Name of the HDF5 group which will be created to contain the metadata
        """
        h5py._errors.silence_errors()
         # needed to supress h5py error printing in child threads.
         # exception handling and logging are used to print
         # more informative messages.

        self._h5_file.create_dataset(_FLAGS_DESCRIPTION_DATASET, data=model_data.flags_description)
        set_telescope_model(self._h5_file, model_data, base_path)
        set_telescope_state(self._h5_file, model_data._telstate,
                            start_timestamp=model_data._start_timestamp)
        self._h5_file.flush()

    def free_space(self):
        """
        Bytes of free space remaining on the file system containing the file.
        """
        stat = os.fstatvfs(self._h5_file.id.get_vfd_handle())
        return stat.f_bsize * stat.f_bavail

    def close(self):
        self._h5_file.close()
        self._h5_file = None
