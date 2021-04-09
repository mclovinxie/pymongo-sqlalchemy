from __future__ import absolute_import

import logging
import sys

logger = logging.getLogger('pymongodb_DataFrame')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(filename)s - %(lineno)d - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
