from sparcur.cli import Main, Options, __doc__ as clidoc
from docopt import parse_defaults

defaults = parse_defaults(clidoc)
args = {'server': True,
        '--latest': True,
        '--sort-count-desc', True}
options = Options(args, defaults)
main = Main(options)

app = main.server(run=False)
