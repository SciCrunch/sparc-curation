from flask import Flask, request, url_for
from htmlfn import htmldoc, atag
from htmlfn import table_style, navbar_style
from sparcur.curation import Integrator


def nowrap(class_, tag=''):
    return (f'{tag}.{class_}'
            '{ white-space: nowrap; }')


def wrap_tables(*tables, title=None):
    return htmldoc(*tables,
                   styles=(table_style, nowrap('td', 'col-id')),
                   title=title)


def make_app(self, name='spc-server'):
    """ self is a sparcur.cli.Main """
    app = Flask(name)
    yield app

    bp = '/dashboard'

    @app.route(f'{bp}/datasets')
    def route_datasets(id):
        table, title = self._print_paths(self.project_path.children)
        return wrap_tables(table, title=title)

    @app.route(f'{bp}/datasets/<id>')
    def route_datasets_id(id):
        if id not in self.dataset_index:
            return abort(404)

        dataset = self.dataset_index[id]
        tables = []
        try:
            ddt = [['TO', 'DO'], [id, 'derive tables from curation export!']]
            table, _ = self._print_table(ddt)
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
            'keywords',
            'subjects',
            'errors',
            'terms',
        )
        report_links = [atag(url_for(f'route_reports_{rn}'), rn) + '<br>\n'
                        for rn in report_names]
        return htmldoc('Reports<br>\n',
                       *report_links,
                       title='Reports')

    @app.route(f'{bp}/reports/completeness')
    def route_reports_completeness():
        table, title = self.report.completeness()
        return wrap_tables(table, title=title)

    @app.route(f'{bp}/reports/size')
    def route_reports_size():
        table, title = self.report.size(dirs=self.project_path.children)
        return wrap_tables(table, title=title)

    @app.route(f'{bp}/reports/filetypes')
    def route_reports_filetypes():
        tables = []
        for table, title in self.report.filetypes():
            tables.append(table + '<br>\n')

        return wrap_tables(*tables, title='Filetypes')

    @app.route(f'{bp}/reports/keywords')
    def route_reports_keywords():
        table, title = self.report.keywords()
        return wrap_tables(table, title=title)

    @app.route(f'{bp}/reports/subjects')
    def route_reports_subjects():
        table, title = self.report.subjects()
        return wrap_tables(table, title=title)

    @app.route(f'{bp}/reports/errors')
    def route_reports_errors():
        return 'TODO'
        table, title = self.report.errors()
        return wrap_tables(table, title=title)

    @app.route(f'{bp}/reports/terms')
    def route_reports_terms():
        tables = []
        for table, title in self.report.terms():
            tables.append(table + '<br>\n')

        return wrap_tables(*tables, title='Terms')
