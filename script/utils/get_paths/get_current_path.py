# -*- coding: utf-8 -*-
"""
 Fonction get_current_path: path du folder du script
"""

import os
import pathlib
from pathlib import Path


def get_current_path() -> Path:
    return pathlib.Path(os.getcwd())
