import pathlib
#from xml.etree import ElementTree as etree  # pypy3 2m18.445s  # 3.7 1m56.417s  # WAT
from lxml import etree  # python3.7 0m38.388s
from . import schemas as sc
from .core import HasErrors
from .utils import log, logd

if etree.__name__ == 'lxml.etree':
    parser = etree.XMLParser(
        collect_ids=False,
        #remove_blank_text=True,
    )


class XmlSource:

    def __init__(self, path):
        self.path = path
        if etree.__name__ == 'lxml.etree':
            # FIXME this version is much faster but on a completely run of the
            # mill workload uses 4 gigs of memory vs 360mb for pure python
            # possibly relevant
            # https://benbernardblog.com/tracking-down-a-freaky-python-memory-leak-part-2/
            self.e = etree.parse(self.path.as_posix(), parser=parser)
            self.namespaces = {k if k else '_':v
                               for k, v in next(self.e.iter()).nsmap.items()}
        else:
            self.e = etree.parse(self.path)
            self.namespaces = dict([node for (_, node) in
                                    etree.iterparse(self.path,
                                                    events=['start-ns'])])
            if '' in self.namespaces:
                self.namespaces['_'] = self.namespaces.pop('')

        self.xpath = self.mkx(self.e)

    if etree.__name__ == 'lxml.etree':
        def __del__(self):
            # slows things down a bit but saves a ton of
            # memory when parsing multiple files
            self.e.getroot().clear()
            del self.e

    def mkx(self, element):
        if hasattr(element, 'xpath'):
            def xpath(path, e=element):
                return e.xpath(path, namespaces=self.namespaces)
        else:
            def xpath(path, e=element):
                if path.startswith('@'):
                    attribute = path.strip('@')
                    return e.get(attribute)

                elif '/@' in path:
                    path, attribute = path.split('/@')
                    return [m.get(attribute)
                            for m in e.findall(path, namespaces=self.namespaces)]

                elif path.endswith('/text()'):
                    path, _ = path.rsplit('/', 1)
                    return [m.text
                            for m in e.findall(path, namespaces=self.namespaces)]

                else:
                    return e.findall(path, namespaces=self.namespaces)

        return xpath

    def asDict(self):  # FIXME data vs asDict
        return self._condense()

    def _condense(self):
        if hasattr(self.asDict, 'schema'):
            _schema = self.asDict.schema.schema
        else:
            _schema = None

        # FIXME how to deal with combination bits in schema, ignore for now

        et = tuple()
        def condense(thing, schema=_schema):

            #print(thing, schema)
            if isinstance(thing, dict):
                nschema = schema['properties'] if 'properties' in schema else et
                return {k:nv
                        for k, v in thing.items()
                        for nv in (condense(v, nschema[k] if k in nschema else et),)
                        # stick with the pattern to filter null fields
                        # we probably don't need to work in reverse
                        # for this format
                        if nv is not None}

            elif isinstance(thing, list):
                nschema = schema['items'] if 'items' in schema else et
                if not thing:
                    if sc.not_array(schema):
                        return None
                    else:
                        return thing
                else:
                    condensed = [condense(v, nschema) for v in thing]
                    if len(condensed) == 1 and sc.not_array(schema):
                        return condensed[0]
                    else:
                        # by default if we lack type information don't unpack
                        # i.e., don't destroy anything unless we are told it is ok
                        return condensed
            else:
                return thing

        data_in = self._extract()
        data_out = condense(data_in)
        for k, v in tuple(data_out.items()):
            if not v:
                data_out.pop(k)

        return data_out

    def _extract(self):
        raise NotImplementedError('implement in subclasses')


hasSchema = sc.HasSchema()
@hasSchema.mark
class ExtractMBF(XmlSource, HasErrors):

    @hasSchema.f(sc.MbfTracingSchema)
    def asDict(self):
        data_in = self._condense()
        id = self.path.cache.uri_api
        #data_in['id'] = id  # FIXME how to approach file level metadata and manifest information
        # TODO
        return data_in

    def _extract(self):
        subject = {
            'id':      self.xpath('_:sparcdata/_:subject/@subjectid'),
            'species': self.xpath('_:sparcdata/_:subject/@species'),
            'sex':     self.xpath('_:sparcdata/_:subject/@sex'),
            'age':     self.xpath('_:sparcdata/_:subject/@age'), # age at death ? (maximum age heh)
        }

        atlas = {
            'organ':        self.xpath('_:sparcdata/_:atlas/@organ'),
            'atlas_label':  self.xpath('_:sparcdata/_:atlas/@label'),
            'atlas_rootid': self.xpath('_:sparcdata/_:atlas/@rootid'),
        }

        images = [
            {'path_mbf': [pathlib.PureWindowsPath(p) for p in image_xpath('_:filename/text()')],
             'channels': [{'id':     channel_xpath('@id'),
                           'source': channel_xpath('@source'),}
                          for channel in image_xpath('_:channels/_:channel')
                          for channel_xpath in (self.mkx(channel),)],
            }
            for image in self.xpath('_:images/_:image')
            for image_xpath in (self.mkx(image),)
        ]

        # these are probably study targets ? or rois ? or what ...
        contours = [
            {'name':        xpath('@name'),
             'guid':        xpath('_:property[@name="GUID"]/_:s/text()'),
             'id_ontology': xpath('_:property[@name="TraceAssociation"]/_:s/text()'),
             # closed=true ??
            }
            for contour in self.xpath('_:contour')
            for xpath in (self.mkx(contour),)]

        return {
            'subject':  subject,
            'atlas':    atlas,
            'images':   images,
            'contours': contours,
        }
