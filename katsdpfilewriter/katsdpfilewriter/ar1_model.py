"""
Interim replacement that moves someway from the static configuration of
RTS to a semi-dynamic model that uses the antenna-mask to add Antenna
components. CBF and other devices still static whilst we develop
kattelmod
"""

from .telescope_model import TelescopeComponent, TelescopeModel

# Component Definitions

class AntennaPositioner(TelescopeComponent):
    def __init__(self, *args, **kwargs):
        super(AntennaPositioner, self).__init__(*args, **kwargs)
        self.add_sensors(
                ['activity', 'target',
                 'pos_request_scan_azim', 'pos_request_scan_elev',
                 'pos_actual_scan_azim', 'pos_actual_scan_elev',
                 'dig_noise_diode', 'ap_indexer_position',
                 'rsc_rxl_serial_number', 'rsc_rxs_serial_number',
                 'rsc_rxu_serial_number', 'rsc_rxx_serial_number'], True)
        self.add_attributes(['observer'], True)
        self.add_attributes(['description'], False)


class CorrelatorBeamformer(TelescopeComponent):
    def __init__(self, *args, **kwargs):
        super(CorrelatorBeamformer, self).__init__(*args, **kwargs)
        self.add_sensors(['target'], True)
        self.add_sensors(['auto_delay_enabled'], False)
        self.add_attributes(
                ['n_chans', 'n_accs', 'n_bls', 'bls_ordering', 'bandwidth',
                 'sync_time', 'int_time', 'scale_factor_timestamp'], True)
        self.add_attributes(['center_freq'], False)


class Enviro(TelescopeComponent):
    def __init__(self, *args, **kwargs):
        super(Enviro, self).__init__(*args, **kwargs)
        self.add_sensors(
                ['air_pressure', 'air_relative_humidity', 'air_temperature',
                 'mean_wind_speed', 'wind_direction'])


class Digitiser(TelescopeComponent):
    def __init__(self, *args, **kwargs):
        super(Digitiser, self).__init__(*args, **kwargs)
        self.add_sensors(['overflow'])


class Observation(TelescopeComponent):
    def __init__(self, *args, **kwargs):
        super(Observation, self).__init__(*args, **kwargs)
        self.add_sensors(['label', 'params'], True)
        self.add_sensors(['script_log'], False)


class SDP(TelescopeComponent):
    def __init__(self, *args, **kwargs):
        super(SDP, self).__init__(*args, **kwargs)
        self.add_attributes(['l0_int_time'], True)


def create_model(antenna_mask=[]):
    components = []
    for ant_name in antenna_mask:
        components.append(AntennaPositioner(name=ant_name))
    cbf = CorrelatorBeamformer(name='cbf')
    env = Enviro(name='anc')
    obs = Observation(name='obs')
    sdp = SDP(name='sdp')
    components.extend([cbf, env, obs, sdp])

    model = TelescopeModel()
    model.add_components(components)
    model.set_flags_description([
        ('reserved0', 'reserved - bit 0'),
        ('static', 'predefined static flag list'),
        ('cam', 'flag based on live CAM information'),
        ('reserved3', 'reserved - bit 3'),
        ('ingest_rfi', 'RFI detected in ingest'),
        ('predicted_rfi', 'RFI predicted from space based pollutants'),
        ('cal_rfi', 'RFI detected in calibration'),
        ('reserved7', 'reserved - bit 7')
    ])
    return model
