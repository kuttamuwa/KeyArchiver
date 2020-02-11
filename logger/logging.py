# -*- coding: utf-8 -*-
# # !/usr/bin/env python

# Author / Designer : Umut Ucok

import logging
import os

# from logging import _levelNames

__loggingoutput__ = os.path.abspath('.')


class LoggingObject:
    logging_levels = {'NOTSET': logging.NOTSET, 'DEBUG': logging.DEBUG, 'INFO': logging.INFO,
                      'WARNING': logging.WARNING, 'ERROR': logging.ERROR, 'CRITICAL': logging.CRITICAL,
                      'WARN': logging.WARN}

    def __init__(self, logging_name, setlevel, output):
        # self.level_check(setlevel)
        self.name = logging_name
        self.output = self.check_output(output)
        self.level = self.level_check(setlevel)

        self.logger = logging.getLogger(logging_name)
        self.logger.setLevel(self.level)  # logging.DEBUG

        # file handler
        self.fh = logging.FileHandler(self.output)
        self.fh.setLevel(self.logger.level)

        # console handler
        self.ch = logging.StreamHandler()
        self.ch.setLevel(self.level)  # logging.ERROR

        # set format
        self.set_formatter()

        # adding handlers
        self.adding_handlers()

        # first job for testing
        # self.first_job()

    def check_output(self, output):
        if not output.endswith(".log"):
            print("You gave folder for output.")
            output = "{}\\{}.log".format(output, self.name)

        return output

    @staticmethod
    def get_warning_level():
        return logging.WARNING

    @staticmethod
    def get_debug_level():
        return logging.DEBUG

    @staticmethod
    def get_error_level():
        return logging.ERROR

    @staticmethod
    def get_info_level():
        return logging.INFO

    def set_formatter(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fh.setFormatter(formatter)
        self.ch.setFormatter(formatter)

    def adding_handlers(self):
        self.logger.addHandler(self.fh)
        self.logger.addHandler(self.ch)

    @classmethod
    def level_check(cls, level):
        """
        :param level: string. You dont know logging object properties. you may type :
        NOTSET, DEBUG, INFO, WARNING, WARN (WTF?) ERROR, CRITICAL
        :return: logging level object: right one.
        """
        if type(level) != str:
            raise AssertionError("Level param must be string !")

        level = level.upper()

        try:
            return cls.logging_levels[level]

        except KeyError:
            raise KeyError("Probably your level name is not in logging level names. So please check it : \n"
                           "{}".format([i for i in cls.logging_levels if type(i) != int]))

    def first_job(self):
        self.logger.info("{} named logging has been started ! ".format(self.name))

    def log_info(self, text, **kwargs):
        self.logger.info(text, **kwargs)

    def log_debug(self, text, **kwargs):
        self.logger.debug(text, **kwargs)

    def log_warning(self, text, **kwargs):
        self.logger.warning(text, **kwargs)

    def log_critical(self, text, **kwargs):
        self.logger.critical(text, **kwargs)

    def log_error(self, text, **kwargs):
        self.logger.error(text, **kwargs)

    def log_warn(self, text, **kwargs):
        self.log_warning(text, **kwargs)
