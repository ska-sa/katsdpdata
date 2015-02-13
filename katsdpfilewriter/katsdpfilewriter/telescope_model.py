"""
Defines the expected and required sensors and attributes for a telescope.

This is purely declarative and does not store any values. Logic for loading and
saving lives elsewhere.
"""

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Attribute(object):
    def __init__(self, name, full_name, critical=False):
        self.name = name
        self.full_name = full_name
        self.critical = critical

class Sensor(object):
    def __init__(self, name, full_name, critical=False, description=None):
        self.name = name
        self.full_name = full_name
        self.critical = critical
        self.description = description

class TelescopeComponent(object):
    def __init__(self, name, proxy_path=None):
        self.name = name
        self.proxy_path = proxy_path if proxy_path is not None else name
        self.sensors = []
        self.attributes = []

    def add_sensors(self, names, critical=False):
        for name in names:
            self.sensors.append(Sensor(
                name, '{0}_{1}'.format(self.proxy_path, name), critical))

    def add_attributes(self, names, critical=False):
        for name in names:
            self.attributes.append(Attribute(
                name, '{0}_{1}'.format(self.proxy_path, name), critical))

class TelescopeModel(object):
    def __init__(self):
        self.components = {}
        self.flags_description = []

    @classmethod
    def enable_debug(cls, debug=True):
        if debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

    def add_components(self, components):
        for component in components:
            if self.components.has_key(component):
                logger.warning("Component name %s is not unique", component.name)
                continue
            self.components[component.name] = component
        logger.debug("Added %d components to model.", len(self.components))

    def set_flags_description(self, flags_description):
        """Set names and descriptions for flags. `flags_description` is a list of (name, description)
        tuples."""
        self.flags_description = flags_description
