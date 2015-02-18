"""
Writes L0 data plus metadata to an HDF5 file.
"""

import logging
import h5py
import numpy as np

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


class File(object):
    def __init__(self, filename):
        """Initialises an HDF5 output file as appropriate for this version of
        the telescope model."""
        h5_file = h5py.File(filename, mode="w")
        h5_file['/'].create_group('Data')
        h5_file['/'].attrs['version'] = HDF5_VERSION
        self._h5_file = h5_file
        self._created_data = False

    def set_timestamps(self, timestamps):
        """Write all timestamps for the file in one go. This must only be
        called once, as it creates the dataset.
        """
        self._h5_file.create_dataset(_TIMESTAMPS_DATASET, data=timestamps)

    def _create_data(self, shape):
        """Creates the data sets for visibilities and flags."""
        shape = list(shape)  # Ensures that + works belows
        self._h5_file.create_dataset(
                _CBF_DATA_DATASET, [0] + shape + [2],
                maxshape=[None] + shape + [2], dtype=np.float32)
        self._h5_file.create_dataset(
                _FLAGS_DATASET, [0] + shape,
                maxshape=[None] + shape, dtype=np.uint8)
        self._created_data = True

    def add_data_frame(self, vis, flags):
        """Add a single visibility/flags frame to the file. The datasets are
        created on first use.

        Parameters
        ----------
        vis : numpy array, complex64, dimensions channels and baselines
            Visibilities
        flags : numpy array, uint8, dimensions channels and baselines
            Flags
        """
        # create datasets if they do not already exist
        if not self._created_data:
            self._create_data(vis.shape)

        # resize datasets
        h5_cbf = self._h5_file[_CBF_DATA_DATASET]
        h5_flags = self._h5_file[_FLAGS_DATASET]
        idx = h5_cbf.shape[0]
        h5_cbf.resize(idx+1, axis=0)
        h5_flags.resize(idx+1, axis=0)

        # Complex values are written to file as an extra dimension of size 2,
        # rather than as structs. Revisit this later to see if either the HDF5
        # file format can be changed to store complex data (rather than
        # having a real/imag axis for reals).
        vis_pairs = _split_array(vis, np.float32)
        h5_cbf[idx] = vis_pairs
        h5_flags[idx] = flags
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
        for component in model_data.components.values():
            comp_base = "{0}/{1}/".format(base_path, component.name)
            try:
                c_group = self._h5_file.create_group(comp_base)
                c_group.attrs['class'] = str(component.__class__.__name__)
            except ValueError:
                c_group = self._h5_file[comp_base]
                logger.warning("Failed to create group %s (likely to already exist)", comp_base)
            for attribute in component.attributes:
                value = model_data.get_attribute_value(attribute)
                if value is not None:
                    c_group.attrs[attribute.name] = value
            for sensor in sorted(component.sensors, key=lambda sensor: sensor.name):
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
        self._h5_file.flush()

    def close(self):
        self._h5_file.close()
        self._h5_file = None
