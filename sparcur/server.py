from flask import Flask, request, url_for
from htmlfn import htmldoc, atag
from htmlfn import table_style, navbar_style


def wrap_tables(*tables, title=None):
    return htmldoc(*tables,
                   styles=(table_style,),
                   title=title)


def make_app(self, name='spc-server'):
    """ self is a sparcur.cli.Main """
    app = Flask(name)
    yield app

    @app.route('/datasets')
    def route_datasets(id):
        table, title = self._print_paths(self.project_path.children)
        return wrap_tables(table, title=title)

    @app.route('/datasets/<id>')
    def route_datasets_id(id):
        if id not in self.dataset_index:
            return abort(404)

        dataset = self.dataset_index[id]
        tables = []
        try:
            ddt = next(dataset.dataset_description).t
            table, _ = self._print_table(ddt)
            tables.append(table)
        except StopIteration:
            return abort(404)  # FIXME ... no data instead plus iterate

        return wrap_tables(*tables, title='Dataset metadata tables')

    @app.route('/reports')
    @app.route('/reports/')
    def route_reports():
        report_names = (
            'completeness',
            'size',
            'filetypes',
        )
        report_links = [atag(url_for(f'route_reports_{rn}'), rn) + '<br>\n'
                        for rn in report_names]
        return htmldoc('Reports<br>\n',
                       *report_links,
                       title='Reports')

    @app.route('/reports/completeness')
    def route_reports_completeness():
        table, title = self.report.completeness()
        return wrap_tables(table, title=title)

    @app.route('/reports/size')
    def route_reports_size():
        table, title = self.report.size(dirs=self.project_path.children)
        return wrap_tables(table, title=title)

    @app.route('/reports/filetypes')
    def route_reports_filetypes():
        tables = []
        for table, title in self.report.filetypes():
            tables.append(table + '<br>\n')

        return wrap_tables(*tables, title='Filetypes')

