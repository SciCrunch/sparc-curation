from pathlib import Path
from flask import Flask, request, url_for
import htmlfn as hfn
from htmlfn import htmldoc, atag
from htmlfn import table_style, navbar_style
from pyontutils import clifun as clif
from sparcur import pipelines as pipes
from sparcur.curation import Integrator
from sparcur.utils import log

log = log.getChild('server')

clif.Dispatcher.url_for = staticmethod(url_for)


def nowrap(class_, tag=''):
    return (f'{tag}.{class_}'
            '{ white-space: nowrap; }')


def wrap_tables(*tables, title=None):
    return htmldoc(*tables,
                   styles=(table_style, nowrap('td', 'col-id')),
                   title=title)


def get_dataset_index(data):
    {d['id']:d for d in data['datasets']}

    
def make_app(report, name='spc-server'):
    app = Flask(name)
    yield app

    bp = '/dashboard'

    @app.route(f'{bp}/datasets')
    def route_datasets(id=None):
        # TODO improve this to pull from meta add uris etc.
        table, title = report.size()
        return wrap_tables(table, title=title)

    @app.route(f'{bp}/datasets/<id>')
    @app.route(f'{bp}/datasets/<id>/ttl')
    @app.route(f'{bp}/datasets/<id>/json')
    def route_datasets_id(id, ext=None):
        data = report._data_ir()
        dataset_index = get_dataset_index(data)
        if id not in dataset_index:
            return abort(404)

        dataset = dataset_index[id]
        tables = []
        try:
            ddt = [['TO', 'DO'], [id, 'derive tables from curation export!']]
            table, _ = report._print_table(ddt)
            tables.append(table)
        except StopIteration:
            return abort(404)  # FIXME ... no data instead plus iterate

        return wrap_tables(*tables, title='Dataset metadata tables')

    @app.route(f'{bp}/reports')
    @app.route(f'{bp}/reports/')
    def route_reports():
        report_names = (
            'completeness',
            'size',
            'filetypes',
            'pathids',
            'keywords',
            'samples',
            'subjects',
            'errors',
            'terms',
            'contributors',
        )
        report_links = [atag(url_for(f'route_reports_{rn}', ext=None), rn) + '<br>\n'
                        for rn in report_names]
        return htmldoc('Reports<br>\n',
                       *report_links,
                       title='Reports')

    @app.route(f'{bp}/reports/completeness')
    @app.route(f'{bp}/reports/completeness<ext>')
    def route_reports_completeness(ext=wrap_tables):
        return report.completeness(ext=ext)

    @app.route(f'{bp}/reports/size')
    @app.route(f'{bp}/reports/size<ext>')
    def route_reports_size(ext=wrap_tables):
        return report.size(ext=ext)

    @app.route(f'{bp}/reports/filetypes')
    @app.route(f'{bp}/reports/filetypes<ext>')
    def route_reports_filetypes(ext=None):
        return 'TODO reimplement from path metadata.'
        if ext is not None:  # TODO
            return 'Not found', 404

        tables = []
        for table, title in report.filetypes():
            tables.append(table + '<br>\n')

        return wrap_tables(*tables, title='Filetypes')

    @app.route(f'{bp}/reports/pathids')
    @app.route(f'{bp}/reports/pathids<ext>')
    def route_reports_pathids(ext=wrap_tables):
        return 'Needs to be reimplemented from path metadata if we still want it.'
        #return report.pathids(ext=ext)

    @app.route(f'{bp}/reports/keywords')
    @app.route(f'{bp}/reports/keywords<ext>')
    def route_reports_keywords(ext=wrap_tables):
        return report.keywords(ext=ext)

    @app.route(f'{bp}/reports/samples')
    @app.route(f'{bp}/reports/samples<ext>')
    def route_reports_samples(ext=wrap_tables):
        return report.samples(ext=ext)

    @app.route(f'{bp}/reports/subjects')
    @app.route(f'{bp}/reports/subjects<ext>')
    def route_reports_subjects(ext=wrap_tables):
        return report.subjects(ext=ext)

    @app.route(f'{bp}/reports/errors')
    @app.route(f'{bp}/reports/errors<ext>')
    def route_reports_errors(ext=wrap_tables):
        return 'TODO'
        table, title = report.errors()
        return wrap_tables(table, title=title)

    @app.route(f'{bp}/reports/errors/<id>')
    @app.route(f'{bp}/reports/errors/<id>.<ext>')
    def route_reports_errors_id(id, ext=wrap_tables):
        tables, formatted_title, title = report.errors(id=id)
        log.info(id)
        if tables is None:
            return 'Not found', 404
        return wrap_tables(formatted_title, *tables, title=title)

    @app.route(f'{bp}/reports/terms')
    @app.route(f'{bp}/reports/terms<ext>')
    def route_reports_terms(ext=None):
        if ext is not None:  # TODO
            return 'Not found', 404

        tables = []
        for table, title in report.terms():
            tables.append(hfn.h2tag(title) + '<br>\n')
            tables.append(table + '<br>\n')

        return wrap_tables(*tables, title='Terms')

    @app.route(f'{bp}/reports/contributors')
    @app.route(f'{bp}/reports/contributors<ext>')
    def route_reports_contributors(ext=None):
        return report.contributors(ext=ext)

    @app.route(f'{bp}/apinat/demo')
    @app.route(f'{bp}/apinat/demo<ext>')
    def route_apinat_demo(ext=None):
        source = Path('~/ni/sparc/apinat/sources/').expanduser()  # FIXME config probably
        rm = pipes.ApiNATOMY(source / 'apinatomy-resourceMap.json')
        r = pipes.ApiNATOMY_rdf(rm.data)  # FIXME ... should be able to pass the pipeline
        if ext == '.ttl':
            return r.data.ttl, 200, {'Content-Type': 'text/turtle; charset=utf-8',}

        return hfn.htmldoc(r.data.ttl_html,
                           styles=(hfn.ttl_html_style,),
                           title='ApiNATOMY demo')

    @app.route(f'{bp}/reports/access')
    @app.route(f'{bp}/reports/access<ext>')
    def route_reports_access(ext=wrap_tables):
        return report.access(ext=ext)

    @app.route(f'{bp}/run/datasets/')
    @app.route(f'{bp}/run/datasets/<id>')
    def route_run_datasets(id=None):
        # TODO permissioning
        if id is None:
            pass

        # TODO send a message to/fork a process to run an export of a specific dataset
