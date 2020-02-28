import pathlib
from lxml import etree
from .core import HasErrors
from .utils import log, logd


class XmlSource:

    def __init__(self, path):
        self.path = path
        #parser = etree.XMLParser(remove_blank_text=True)
        self.e = etree.parse(self.path.as_posix())
        self.namespaces = {k if k else '_':v for k, v in next(self.e.iter()).nsmap.items()}
        self.xpath = self.mkx(self.e)

    def mkx(self, element):
        def xpath(path, e=element):
            return e.xpath(path, namespaces=self.namespaces)

        return xpath

    def asDict(self):
        return self._condense()

    def _condense(self):
        def condense(thing):
            if isinstance(thing, dict):
                return {k:condense(v) for k, v in thing.items()}
            elif isinstance(thing, list):
                if not thing:
                    return None
                else:
                    condensed = [condense(v) for v in thing]
                    if len(condensed) == 1:
                        return condensed[0]
                    else:
                        return condensed
            else:
                return thing

        data_in = self._extract()
        data_out = condense(data_in)
        return data_out

    def _extract(self):
        raise NotImplementedError('implement in subclasses')


class ExtractMBF(XmlSource, HasErrors):

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
            {'name': xpath('@name'),
             'guid': xpath('_:property[@name="GUID"]/_:s/text()'),
             'id':   xpath('_:property[@name="TraceAssociation"]/_:s/text()'),
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
