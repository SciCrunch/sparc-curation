import tempfile
from pathlib import Path
import orthauth as oa

#auth = oa.configure(oa.crpath(__file__, 'auth-config.py'))
auth = oa.configure_here('auth-config.py', __name__)


class config:
    organ_html_path = Path('../resources/sparc-nervous-system-graphic.html')  # FIXME include in distribution ...
