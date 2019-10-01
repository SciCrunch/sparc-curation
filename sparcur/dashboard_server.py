import augpathlib as aug
from docopt import parse_defaults
from sparcur import exceptions as exc
from sparcur.cli import Report, Options, __doc__ as clidoc
from sparcur.paths import Path
from sparcur.server import make_app
from sparcur.curation import Summary

project_path = Path.cwd()
if not (project_path / Path._cache_class._local_data_dir).exists():
    raise exc.NotInProjectError(f'{project_path}')

defaults = {o.name:o.value if o.argcount else None for o in parse_defaults(clidoc)}
args = {'server': True,
        '--latest': True,
        '--sort-count-desc': True,
        '--project-path': project_path,
        '--tab-table': False,
        '<path>': [],

        '--verbose': False,
}

options = Options(args, defaults)
report = Report(options)

# set report paths that would normally be populated from Main
report.cwd = options.project_path
report.project_path = options.project_path
report.project_id = project_path.cache.id  # FIXME should not have to do this manually?
report.summary = Summary(options.project_path)

app, *_ = make_app(report, project_path)
app.debug = False

if __name__ == '__main__':
    app.run(host='localhost', port=defaults['--port'], threaded=True)
