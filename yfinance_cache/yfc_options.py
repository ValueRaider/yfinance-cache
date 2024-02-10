from . import yfc_cache_manager as yfcm

import json
import os
from pandas import Timedelta

class NestedOptions:
    def __init__(self, name, data):
        self.__dict__['name'] = name
        self.__dict__['data'] = data

    def __getattr__(self, key):
        return self.data.get(key)

    def __setattr__(self, key, value):
        if self.name == 'max_ages':
            # Type-check value
            Timedelta(value)

        self.data[key] = value
        _option_manager._save_option()

    def __len__(self):
        return len(self.__dict__['data'])

    def __repr__(self):
        return json.dumps(self.data, indent=4)

class OptionsManager:
    def __init__(self):
        d = yfcm.GetCacheDirpath()
        self.option_file = os.path.join(d, 'options.json')
        self._load_option()

    def _load_option(self):
        try:
            with open(self.option_file, 'r') as file:
                self.options = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            self.options = {}

    def _save_option(self):
        with open(self.option_file, 'w') as file:
            json.dump(self.options, file, indent=4)

    def __getattr__(self, key):
        if key not in self.options:
            self.options[key] = {}
        return NestedOptions(key, self.options[key])

    def __repr__(self):
        return json.dumps(self.options, indent=4)

# Global instance
_option_manager = OptionsManager()
