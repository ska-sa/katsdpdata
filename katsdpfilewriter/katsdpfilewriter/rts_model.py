"""
Short-term module that builds a standard telescope model for RTS. This should
be replaced by having the configuration stored in the telescope state.
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
        self.add_attributes(['description'], True)


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


def create_model():
    m063 = AntennaPositioner(name='m063')
    m062 = AntennaPositioner(name='m062')
    m001 = AntennaPositioner(name='m001')
    m000 = AntennaPositioner(name='m000')
    cbf = CorrelatorBeamformer(name='data_1')
    env = Enviro(name='anc')
    obs = Observation(name='obs')
    sdp = SDP(name='sdp')
    model = TelescopeModel()
    model.add_components([m063, m062, cbf, env, obs, sdp])
    model.set_flags_description([
        ('reserved0', 'reserved - bit 0'),
        ('static', 'predefined static flag list'),
        ('cam', 'flag based on live CAM information'),
        ('reserved3', 'reserved - bit 3'),
        ('detected_rfi', 'RFI detected in the online system'),
        ('predicted_rfi', 'RFI predicted from space based pollutants'),
        ('reserved6', 'reserved - bit 6'),
        ('reserved7', 'reserved - bit 7')
    ])
    return model
