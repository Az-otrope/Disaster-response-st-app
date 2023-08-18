import os


ABSOLUTE_PATH = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
DATA = os.path.join(ABSOLUTE_PATH, 'data')
RAW = os.path.join(DATA, 'raw')
PROCESSED = os.path.join(DATA, 'processed')

MODELS = os.path.join(ABSOLUTE_PATH, 'models')
