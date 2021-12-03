#!/usr/bin/env python3
import os
import sys

db_root = os.environ.get('DISBATCH_ROOT', os.path.abspath(os.path.dirname(__file__)))
sys.path.append(db_root)
from disbatch import disBatch

disBatch.main(db_root)
