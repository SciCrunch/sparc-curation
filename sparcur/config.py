import tempfile
from pathlib import Path
import orthauth as oa

auth = oa.configure_here('auth-config.py', __name__)


class config:
    organ_html_path = Path('../resources/sparc-nervous-system-graphic.html')  # FIXME include in distribution ...
