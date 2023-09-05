import os, sys
from pathlib import Path
import settings

BASE_DIR = Path(__file__).resolve().parent

if BASE_DIR not in sys.path:
	sys.path.append(str(BASE_DIR))
	os.environ.setdefault('__RECON_SETTINGS_MODULE__', 'document_reconciliation.settings')

__RECON_SETTINGS_MODULE__ = settings