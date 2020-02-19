from docopt import parse_defaults
from sparcur import exceptions as exc
from sparcur.cli import Report, Options, __doc__ as clidoc
from sparcur.paths import Path, BlackfynnCache
from sparcur.config import auth
from sparcur.server import make_app
from sparcur.backends import BlackfynnRemote
from sparcur.curation import Summary

project_path = Path.cwd()
if not (project_path / Path._cache_class._local_data_dir).exists():
    raise exc.NotInProjectError(f'{project_path}')

defaults = {o.name:o.value if o.argcount else None for o in parse_defaults(clidoc)}
args = {'server': True,
        '--raw': False,
        '--latest': True,
        '--sort-count-desc': True,
        '--project-path': project_path,
        '--tab-table': False,
        '<path>': [],
        '--verbose': False,

        '--export-path': auth.get_path('export-path'),
        '--partial': False,
        '--open': False,
        '--debug': False,
}

options = Options(args, defaults)
report = Report(options)

# set report paths that would normally be populated from Main
report.cwd = options.project_path
report.project_path = options.project_path
report.project_id = project_path.cache.id  # FIXME should not have to do this manually?
report.anchor = project_path.cache
report.summary = Summary(options.project_path)
report._timestamp = None  # FIXME
report._folder_timestamp = None  # FIXME

# set up bfapi
report.BlackfynnRemote = BlackfynnRemote._new(Path, BlackfynnCache)
report.BlackfynnRemote.init(report.project_id)

app, *_ = make_app(report, project_path)
app.debug = False

if __name__ == '__main__':
    app.run(host='localhost', port=defaults['--port'], threaded=True)
