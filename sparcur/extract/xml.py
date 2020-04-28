import pathlib
#from xml.etree import ElementTree as etree  # pypy3 2m18.445s  # 3.7 1m56.417s  # WAT
from lxml import etree  # python3.7 0m38.388s
from pyontutils.utils import subclasses
from .. import schemas as sc
from ..core import HasErrors
from ..utils import log, logd

if etree.__name__ == 'lxml.etree':
    parser = etree.XMLParser(
        collect_ids=False,
        #remove_blank_text=True,
    )


class ExtractXml:
    def __new__(cls, path):
        inst = None
        for c in cls.classes:
            #log.debug(c)
            if inst is not None:
                _inst = inst
                inst = c.fromExisting(inst)
                if inst._isXml:
                    delattr(_inst, 'e')  # prevent __del__ from zapping e
                    del _inst
            else:
                inst = c(path)
                if not inst._isXml:
                    inst = NotXml(path)
                    msg = f'path is not a valid xml file! {path}'
                    if inst.addError(msg, blame='submission', path=path):
                        logd.error(msg)

                    return inst

            if inst.typeMatches():
                return inst
            #else:
                #if hasattr(inst, 'e'):
                    #log.debug(inst.e.getroot().tag)

        else:
            path.xopen()
            breakpoint()
            raise TypeError(f'FIXME converter not implemented for whatever this is {path}')


class NotXml(HasErrors):

    _isXml = False
    mimetype = None

    def __init__(self, path):
        super().__init__()
        self.path = path

    def asDict(self):
        out = {}
        self.embedErrors(out)
        return out

    def typeMatches(self):
        return False


class XmlSource(HasErrors):

    top_tag = object()  # should never be able to == anything
    mimetype = 'application/xml'

    @classmethod
    def fromExisting(cls, existing):
        self = object.__new__(cls)
        self.__dict__ = {k:v for k, v in existing.__dict__.items()}
        return self

    def __init__(self, path):
        super().__init__()
        self.path = path
        if etree.__name__ == 'lxml.etree':
            # FIXME this version is much faster but on a completely run of the
            # mill workload uses 4 gigs of memory vs 360mb for pure python
            # possibly relevant
            # https://benbernardblog.com/tracking-down-a-freaky-python-memory-leak-part-2/
            try:
                self.e = etree.parse(self.path.as_posix(), parser=parser)
                self._isXml = True
            except etree.XMLSyntaxError as e:
                self._isXml = False
                logd.exception(e)
                logd.error(f'error parsing {self.path}')
                return

            self.namespaces = {k if k else '_':v
                               for k, v in next(self.e.iter()).nsmap.items()}
        else:
            try:
                self.e = etree.parse(self.path)
                self._isXml = True
            except etree.ParseError as e:
                self._isXml = False
                logd.exception(e)
                logd.error(f'error parsing {self.path}')
                return

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
            if hasattr(self, 'e'):
                self.e.getroot().clear()
                del self.e

    def isXml(self):
        return self._isXml

    def typeMatches(self):
        return self._isXml and self.e.getroot().tag == self.top_tag

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

    def asDict(self, *args, **kwargs):  # FIXME data vs asDict
        data_in = self._condense(*args, **kwargs)
        self.embedErrors(data_in)
        return data_in

    def _condense(self, *args, **kwargs):
        et = tuple()
        if hasattr(self.asDict, 'schema'):
            _schema = self.asDict.schema.schema
        else:
            _schema = et

        # FIXME how to deal with combination bits in schema, ignore for now
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

        data_in = self._extract(*args, **kwargs)
        data_out = condense(data_in)
        for k, v in tuple(data_out.items()):
            if not v:
                data_out.pop(k)

        return data_out

    def _extract(self, *args, **kwargs):
        raise NotImplementedError('implement in subclasses')


hasSchema = sc.HasSchema()
@hasSchema.mark
class ExtractMBF(XmlSource):

    top_tag = '{http://www.mbfbioscience.com}mbf'
    mimetype = 'application/vnd.mbfbioscience.metadata+xml'

    @hasSchema.f(sc.MbfTracingSchema)
    def asDict(self, unique=True, guid=False):
        return self._condense(unique=unique, guid=guid)

    def _condense(self, unique=True, guid=False):
        data_in = super()._condense(unique=unique, guid=guid)
        if 'contours' in data_in:
            if not guid:
                # remove guid fields if present
                # option to retain if a use case for this appears
                for c in data_in['contours']:
                    c.pop('guid', None)

            if unique:
                # by default only store uniquely identified contours,
                # they may be identified by their list index but we
                # don't need that for this part of the metadata
                data_in['contours'] = [dict(s) for s in
                                       set(frozenset(d.items())
                                           for d in data_in['contours'])]

        return data_in

    def _extract(self, *args, **kwargs):
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


hasSchema = sc.HasSchema()
@hasSchema.mark
class ExtractNeurolucida(ExtractMBF):

    top_tag = '{http://www.mbfbioscience.com/2007/neurolucida}mbf'
    mimetype = 'application/vnd.mbfbioscience.neurolucida+xml'

    @hasSchema.f(sc.NeurolucidaSchema)
    def asDict(self, unique=True, guid=False):
        # pretty sure we can't use super().asDict() here because
        # the schemas will fight with eachother
        data_in = self._condense(unique=unique, guid=guid)
        self.embedErrors(data_in)
        return data_in


class ExtractZen(XmlSource):

    top_tag = 'CellCounter_Marker_File'
    mimetype = 'application/vnd.unknown.zen+xml'  # TODO

    def _extract(self):
        images = [{'path_zen':
                   [pathlib.PurePath(p) for p in
                    self.xpath('Image_Properties/Image_Filename/text()')],}]

        return {'images': images}


class ExtractLAS(XmlSource):
    """ Leica Application Suite format,
        probably internal given lack of xmlns """

    top_tag = 'Data'  # very helpful thanks guys >_<
    mimetype = 'application/vnd.leica.las+xml'  # TODO

    def typeMatches(self):
        return (super().typeMatches() and
                'LAS AF' in self.xpath('/Data/Image/Attachment/@Application'))

    def _extract(self):
        images = [{'path_las':
                   [pathlib.PureWindowsPath(p) for p in
                    self.xpath('ImageDescription/FileLocation/text()')],
                   'channels': []  # TODO
        }]

        return {'images': images}


class ExtractPVScan(XmlSource):
    """ Format for proprietary Prairie View Software made by Bruker Corporation
        Version matters for parsing.
        See https://www.openmicroscopy.org/community/viewtopic.php?f=13&t=7534
        Google search results are garbage when searching from the tags and
        for Prairie View without additional qualifiers specifically software or
        microscopy."""

    top_tag = 'PVScan'  # probably not sufficient ...
    mimetype = 'application/vnd.bruker.pvscan+xml'  # TODO

    def _extract(self):
        # TODO nothing of obvious relevance for metadata
        return {}

class ExtractPVVrecSE(XmlSource):
    """ An auxillary Prairie View file that is referenced from the main PVSync
        record in the VoltageRecording section """

    top_tag = 'VRecSessionEntry'
    mimetype = 'application/vnd.bruker.vrecsessionentry+xml'  # TODO

    def _extract(self):
        # TODO nothing of obvious relevance for metadata
        return {}


class ExtractPVExperiment(XmlSource):
    """ An auxillary Prairie View file that is referenced from the main PVSync
        record in the VoltageOutput section """

    # TODO FIXME gonna need some additional context from somewhere
    # xml files like these (missing xmlns) should flag a warning for
    # bad (solipsistic) data management practices

    top_tag = 'Experiment'  # very helpful xml schema creator guy >_<
    mimetype = 'application/vnd.bruker.experiment+xml'  # TODO

    def _extract(self):
        # TODO nothing of obvious relevance for metadata
        return {}


class ExtractZISRAWSUBBLOCK_METADATA(XmlSource):
    """ This is xml extracted from xml embedded in a
        ZISRAWSUBBLOCK section of a Zeiss czi file """

    top_tag = 'METADATA'  # LOL OH WOW >_< this just keeps getting better
    
    mimetype = 'application/vnd.zeiss.czi.ZISRAWSUBBLOCK.METADATA+xml'  # TODO

    def _extract(self):
        # TODO has time information and stage and focus
        # seems to be using file naming conventions to match
        # sidecar to tiff (urg)
        return {}


ExtractXml.classes = (*[c for c in subclasses(XmlSource)], XmlSource)
