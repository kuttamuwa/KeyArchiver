# -*- coding: utf-8 -*-
# !/usr/bin/env python

# Author : Umut Ucok
# NO COMMENT.

"""
It is pretty useful reading config file (.ini ) object for me. You can use read section and list section to read
configs. Sample config are populated in this module folder also.
"""

import configparser
from os.path import split


class ConfiguresReader:
    __modulepath__, _ = split(__file__)

    def __init__(self, config_path):
        self.config_path = config_path

        self.sections = self.list_sections()
        self.configures = [self.read_section(sec) for sec in self.sections]

    def read_section(self, section):
        sec_dict = {}
        configure = configparser.ConfigParser()
        configure.read(self.config_path)
        options = configure.options(section)
        for opt in options:
            sec_dict[opt] = configure.get(section, opt)

        return sec_dict

    def list_sections(self):
        configure = configparser.ConfigParser()
        configure.read(self.config_path)

        return configure.sections()

    def ordered_list_sections(self, sectionlist):
        return [self.read_section(sec) for sec in sectionlist]

    def format_for_dbconnector(self):
        section = self.read_section('db')
        username, password, dbname, port, ip, _ = section.values()
        path = section.get('path')
        version = section.get('version')
        sde_engine = section.get('sde_engine')

        return username, password, dbname, port, ip, path, version, sde_engine
