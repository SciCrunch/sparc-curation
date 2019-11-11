import tempfile
from pathlib import Path
import orthauth as oa

auth = oa.configure_relative('auth-config.py')


class config:
    organ_html_path = Path('../resources/sparc-nervous-system-graphic.html')  # FIXME include in distribution ...
