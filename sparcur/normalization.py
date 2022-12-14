""" string normalizers, strings that change their content to match a standard """

import numbers
import datetime
from ast import literal_eval
from types import GeneratorType
from html.parser import HTMLParser
import idlib
from pysercomb.pyr.types import Quantity
from . import exceptions as exc
from .core import log, logd, HasErrors
from .core import OntId
from .utils import is_list_or_tuple, levenshteinDistance


BLANK_VALUE = object()
NOT_APPLICABLE = object()


def related_identifiers(rids):
    for rid in rids:
        try:
            out = related_identifier_record(rid)
            yield out
        except Exception as e:
            breakpoint()
            log.exception(e)
            pass


def related_identifier_record(rid):
    #types = sc.DatasetDescriptionExportSchema.schema[
        #'properties']['related_identifiers']['relation_type']['enum']
        #'properties']['related_identifiers']['relation_type']['enum']
    he = HasErrors(pipeline_stage='sparcur.normalization.related_identifier_record')
    for key in ('relation_type', 'related_identifier_type', 'related_identifier'):
        # FIXME this is a case where we we clearly need to have split schema validation
        # where we need to filter failures first and only then normalize and the filtering
        # should be based on the fields that must be present to normalize correctly or
        # something ... unfortunately there isn't a simple way to couple these things
        # declaratively :/
        if key not in rid:
            return rid

    rel = rid['relation_type']
    type = rid['related_identifier_type']
    id = rid['related_identifier']
    desc = (rid['related_identifier_description']
            if 'related_identifier_description' in rid else None)

    if rel == 'HasProtocol':
        try:
            nid = NormDatasetDescriptionFile._protocol_url_or_doi(id)
            id, = protocol_url_or_doi((nid,))
        except Exception as e:
            # XXX we are missing the required context to produce an
            # understanable error here however the result will filter
            # fail later on pointing to the correct file
            if he.addError(str(e)):
                log.exception(e)
            id = None
    else:
        pass

    out = dict(
        relation_type=rel,
        related_identifier_type=type,
    )

    if id is not None and id:
        out['related_identifier'] = id

    if desc is not None and desc:
        out['related_identifier_description'] = desc

    he.embedErrors(out)
    return out


def description(value):
    # handle incorrectly doubly quoted descriptions
    # FIXME TODO issue an error/warning for this
    if value.startswith('"') and value.endswith('"'):
        if '\n' in value:
            _value = '""' + value + '""'
        else:
            _value = value
        try:
            return literal_eval(_value)
        except Exception as e:
            pass

    return value


def protocol_url_or_doi(value):
    # FIXME network sandbox violation
    # FIXME can't use idlib.get_right_id because it is
    # totally broken with regard to dereferencing
    # FIXME under certain circumstances this field winds
    # up with a None in a list, that is really really bad
    # and we need to figure out why/how, I may have fixed
    # it with an update to idlib, but I'm not sure
    # XXX this is happening due to handling and logging errors and then returning
    if not is_list_or_tuple(value):
        value = value,
        out = None
    else:
        out = []

    for v in value:
        if not isinstance(v, idlib.Stream):
            if v in NormValues._na_things:
                raise exc.NotApplicableError(f'na/none value: {v!r}')

            raise exc.CouldNotNormalizeError(f'should already be in stream form {v!r}')

        try:
            normed = v.dereference(idlib.get_right_id)  # TODO general check for resolvability?
        except idlib.exc.MalformedIdentifierError as e:
            logd.error(e)
            # XXX WARNING returning value here might cause unexpcted errors
            # however, normalization should never fail, if it does it should
            # return the value that could not be normalized and then validation
            # will catch the error at a later stage
            return value
        except Exception as e:
            logd.error(e)
            return value

        if isinstance(normed, idlib.Pio):
            try:
                uh = normed.uri_human  # deal with new slugs
                normed = uh.uri_api_int  # integer id preferred if available
            except idlib.exc.IdDoesNotExistError as e:
                logd.error(e)  # we'll catch this again later too
            except idlib.exc.NotAuthorizedError as e:
                logd.error(e)
            except Exception as e:
                breakpoint()
                log.exception(e)  # FIXME don't know what we will get here

        if not isinstance(normed, idlib.Pio) and isinstance(v, idlib.Doi):
            # ensure that regular dois are returned, not their referents
            # if it is a protocol id, use that as primary and reference the doi
            # using hasDoi, because the doi metadata is technically different
            # from the protocol metadata/data
            normed = v

        if out is None:
            return normed

        out.append(normed)

    if len(out) > 1:
        # condense cases where the same id came from multiple sources
        # e.g. dataset description and subjects XXX sigh, not where
        # the problem lies it seems?
        out = list(set(out))

    if isinstance(value, tuple):
        out = tuple(out)  # preserve types prevent errors!

    return out


def rrid(value, self=None):
    if value.strip().lower() in ('unknown', 'uknown'):
        return UNKNOWN
    else:
        try:
            out = idlib.Rrid(value)  # XXX possible encapsulation issue
            return out
        except idlib.exceptions.MalformedIdentifierError as e:
            msg = f'malformed RRID: {value}'
            if self is not None:
                if self.addError(msg,
                                pipeline_stage=self.__class__.__name__,
                                blame='submission',
                                path=self._path):
                    log.error(msg)

            if not value.startswith('RRID:'):
                return rrid('RRID:' + value)


class _Unknown(str):
    def __new__(cls, value=None):
        return str.__new__(cls, 'UNKNOWN')


UNKNOWN = _Unknown()


class NormSimple(str):

    data = tuple()  # really dict

    def __new__(cls, value):
        return str.__new__(cls, cls.normalize(value))

    @classmethod
    def normalize(cls, value, preserve_case=False):
        v = value if preserve_case else value.lower()

        if v in cls.data:
            return cls.data[v]
        elif preserve_case:
            return value
        else:
            return v


class NormAward(NormSimple):
    data = {
        '1 OT2 OD23853': 'OT2OD023853',  # someone's university database stripped a leading zero
        #'grantOT2OD02387101S2': '',
    }
    @classmethod
    def normalize(cls, value):
        _ovalue = value
        value = super().normalize(value, preserve_case=True)
        if 'OT2' in value and 'OD' not in value:
            # one is missing the OD >_<
            log.warning(value)
            value = value.replace('-', '-OD')  # hack

        if not value:  # should be caught and reported by the schemas
            return value

        n = (value
             .strip()
             .replace('-', '-')  # can you spot the difference?
             .replace('(', '')
             .replace(')', '')
             .replace('-01S1', '')
             .replace('-01', '')
             .replace('-02S2', '')
             .replace('-02', '')
             .replace('SPARC', '')
             .replace('NIH-1', '')
             .replace('NIH-', '')
             .replace('-', '')
             .replace('NIH ', '')
             .replace(' ', ''))
        if n[0] in ('1', '3', '5'):
            n = n[1:]

        if n.endswith('S2'):
            n = n[:-2]

        if n.endswith('D23864'):  # FIXME another trailing zero
            log.critical(_ovalue)
            n = n.replace('D23864', 'D023864')

        if n != _ovalue:
            log.debug(f'\n{_ovalue}\n{n}')
        return n


class NormFileSuffix(str):
    data = {
        'jpeg':'jpg',
        'tif':'tiff',
    }

    def __new__(cls, value):
        return str.__new__(cls, cls.normalize(value))

    @classmethod
    def normalize(cls, value):
        v = value.lower()
        ext = v[1:]
        if ext in cls.data:
            return '.' + data[ext]
        else:
            return v


class NormSpecies(NormSimple):
    data = {
        'cat':'Felis catus',
        'rat':'Rattus norvegicus',
        'mouse':'Mus musculus',
        'sus scrofa domestica': 'Sus scrofa domesticus',
        'ferret': 'Mustela putorius furo',

        # spelling errors
        'mus muscus': 'Mus musculus',
        'sus scofa': 'Sus scrofa',

        'rattus noervegicus domestica': 'Rattus norvegicus domestica',
        'rattus norvegicus domestica': 'Rattus norvegicus domestica',
        'ratus norvegicus': 'Rattus norvegicus',
        'norway rat': 'Rattus norvegicus',
        'norway rats': 'Rattus norvegicus',
    }

class NormSex(NormSimple):
    data = {
        'm':'male',
        'f':'female',
    }


class NormHeader(NormSimple):
    __armi = 'age_range_min'
    __arma = 'age_range_max'
    data = {
        'age_range_minimum': __armi,
        'age_range_maximum': __arma,
        'protocol_io_location': 'protocol_url_or_doi',
        'protocol_io_location_2': 'protocol_url_or_doi',
        'protocol_title_2': 'protocol_title',
    }


class NormContributorRole(str):
    values = ('CorrespondingAuthor',
              'ContactPerson',  # XXX need to
              'DataCollector',
              'DataCurator',
              'DataManager',
              'Distributor',
              'Editor',
              'HostingInstitution',
              'PrincipalInvestigator',  # added for sparc map to ProjectLeader probably?
              'CoInvestigator',  # added for sparc, to distingusih ResponsibleInvestigator
              'Creator',  # this is a separate field in datacite so we will need lift on export
              'Producer',
              'ProjectLeader',
              'ProjectManager',
              'ProjectMember',
              'RegistrationAgency',
              'RegistrationAuthority',
              'RelatedPerson',
              'Researcher',
              'ResearchGroup',
              'RightsHolder',
              'Sponsor',
              'Supervisor',
              'WorkPackageLeader',
              'Other',)

    def __new__(cls, value):
        return str.__new__(cls, cls.normalize(value))

    @classmethod
    def normalize(cls, value):
        # a hilariously slow way to do this
        # also not really normalization ... more, best guess for what people were shooting for
        # FIXME warn on this if it does not match so curators can see the value
        if value:
            best = sorted((levenshteinDistance(value, v), v) for v in cls.values)[0]
            distance = best[0]
            normalized = best[1]
            cutoff = len(value) / 2
            if distance > cutoff:
                msg = (f'"{value}" could not be normalized, best was {normalized} '
                       f'with distance {distance} cutoff was {cutoff}')
                raise exc.CouldNotNormalizeError(msg)

            return normalized


# static value normalization for complex inputs


class ATag(HTMLParser):
    text = None
    href = None
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for attr, value in attrs:
                if attr == 'href':
                    self.href = value

    def handle_endtag(self, tag):
        #print("Encountered an end tag :", tag)
        pass

    def handle_data(self, data):
        self.text = data
        #print("Encountered some data  :", data)

    def asJson(self, input):
        self.feed(input)
        if self.text is not None:
            return {'href': self.href, 'text': self.text}
        else:
            return self.href


class NormValues(HasErrors):
    """ Base class with an open dir to avoid name collisions """

    embed_bad_key_message = False  # TODO probably don't want this, better to zap bad values in pipeline

    _na_things = ('NA', 'n/a', 'N/A', 'none', 'None', 'no protocols', 'na')

    def __init__(self, obj_inst):
        super().__init__()
        self._obj_inst = obj_inst

    def _bind(self):
        self._record_type_key_header = self._obj_inst.record_type_key_header
        self._norm_to_orig_alt = self._obj_inst.norm_to_orig_alt
        self._norm_to_orig_header = self._obj_inst.norm_to_orig_header
        self._groups_alt = self._obj_inst.groups_alt
        self._path = self._obj_inst.path

    def _error_on_na(self, value, key=None):
        """ N/A -> raise for cases where it should just be removed """
        if isinstance(value, str):
            v = value.strip()
            if v in self._na_things:
                # TODO consider double checking these cases ?
                raise exc.NotApplicableError(key)

    def _deatag(self, value):
        if value.startswith('<a') and value.endswith('</a>'):
            at = ATag()
            j = at.asJson(value)
            return at.href, j

        return value, None

    @staticmethod
    def _query(value, prefix):
        raise NotImplementedError('We do not want to hit the network here!')
        for query_type in ('term', 'search'):
            terms = list(OntTerm.query(prefix=prefix, **{query_type:value}))
            if terms:
                #print('matching', terms[0], value)
                #print('extra terms for', value, terms[1:])
                return terms[0]
            else:
                continue

        else:
            log.warning(f'No ontology id found for {value}')
            return value

    def _normv(self, thing, key=None, rec=None, path=tuple()):
        cell_errors = []
        #if 'software_rrid' in path:
            #breakpoint()
        def skipt(th, _types=(numbers.Number, datetime.datetime, datetime.date, datetime.time)):
            # FIXME _types should cover all the extraneous types that thing could take on
            # specifically those produced by openpyxl
            for t in _types:
                if isinstance(th, t):
                    return True

        if isinstance(thing, dict):
            out = {}
            for i, (k, v) in enumerate(thing.items()):
                intermediate = k in self._norm_to_orig_header
                process_group = k in self._groups_alt
                if k == self._record_type_key_header:
                    gnv = lambda : v
                elif intermediate:
                    gnv = lambda : self._normv(v, key, rec, path)
                elif k in self._norm_to_orig_alt:
                    gnv = lambda : self._normv(v, k, i, path + (k,))
                elif process_group:
                    gnv = lambda : self._normv(v, k, i, path + (k,))
                else:
                    raise ValueError(f'what is going on here?! {k} {v}')

                try:
                    nv = gnv()
                    out[k] = nv
                except exc.NotApplicableError:
                    pass
                except exc.TabularCellError as e:
                    # strip the error and log it
                    out[k] = e.value
                    # since out is mutable here it will continue to update
                    if rec is None or process_group:  # aka rec is None
                        if rec is None:
                            kmsg = f'.{k}'
                        else:
                            kmsg = f'.{key}'

                        if self.addError(str(e),
                                         pipeline_stage=f'{self.__class__.__name__}{kmsg}',
                                         blame='submission'):
                            log.critical(e)
                        #if isinstance(out[k], dict):
                            #self.embedLastError(out[k])

                    else:
                        if not intermediate and self.embed_bad_key_message:
                            self.errorInKey(out, k)

                        location = rec, i
                        new_e = exc.TabularCellError(str(e), value=out, location=location)
                        cell_errors.append(new_e)

            # end for
            if cell_errors:
                if len(cell_errors) != 1:
                    e = exc.TabularCellError(str(cell_errors),
                                             value=out,
                                             location=[e.location for e in cell_errors])
                else:
                    e = cell_errors[0]

                raise e 

            return out

        elif is_list_or_tuple(thing):
            # normal json not the tabular conversion case
            # or arrays at the bottom
            out = []
            _errors = []
            for i, v in enumerate(thing):
                try:
                    o = self._normv(v, key, i, path + (list,))
                    out.append(o)
                except exc.TabularCellError as e:
                    out.append(None)  # will be replaced in the if errors block
                    _errors.append((i, e))

            errors = [(i, e) for i, e in enumerate(out)
                      if isinstance(e, exc.TabularCellError)]
            errors = errors + _errors

            if errors:
                cell_errors = [e for i, e in errors]
                for i, e in errors:
                    out[i] = e.value

                e = exc.TabularCellError(str(cell_errors),
                                         value=out,
                                         location=[e.location for e in cell_errors])
                raise e

            return out

        elif skipt(thing):
            # this way we don't have to check the schema here
            # we avoid an AttributeError in the event we treated
            # a number like a string, and if it is incorrect we
            # catch it later with the schemas
            return thing
        else:
            # TODO make use of path
            # FIXME I do NOT like this pattern :/

            if isinstance(thing, str):
                self._error_on_na(thing, key)  # TODO see if this makes sense

            if isinstance(key, str) and hasattr(self, key):

                out = getattr(self, key)(thing)

                if isinstance(out, GeneratorType):
                    out = tuple(out)
                    errors = [(i, e) for i, e in enumerate(out)
                              if isinstance(e, exc.TabularCellError)]

                    if errors:
                        cell_errors = [e for i, e in errors]
                        out = [_ for _ in out]
                        for i, e in errors:
                            out[i] = e.value

                        out = tuple(out)
                        e = exc.TabularCellError(str(cell_errors),
                                                 value=out,
                                                 location=[e.location for e in cell_errors])
                        # FIXME this appears to cause double wrapping in tuples again >_<
                        raise e

                    if (len(out) == 1 and (key in self._obj_inst._expect_single or
                                           isinstance(out[0], Quantity))
                        or path[-1] == list):
                        # lists of lists might encounter issues here, but we almost never
                        # encounter those cases with metadata, especially in the tabular conversion
                        # but keep an eye out in which case we can check the schema type
                        # FIXME bad test around Quantity
                        out = out[0]
                    elif not out:
                        if isinstance(thing, str):
                            thing = thing.strip() # sigh

                        if thing is not None and thing != '':
                            msg = f'Normalization {key} returned None for input "{thing}"'
                            if self.addError(msg,
                                             pipeline_stage=f'{self.__class__.__name__}.{key}',
                                             blame='pipeline',):
                                log.critical(msg)

                        out = None

            else:
                out = thing

            if isinstance(out, exc.TabularCellError):
                raise out

            return out

    @property
    def data(self):
        #nk = self._obj_inst._normalize_keys()
        data_in = self._obj_inst._clean()
        self._bind()
        data_out = self._normv(data_in)
        self.embedErrors(data_out)
        return data_out



class NormSubmissionFile(NormValues):

    def milestone_achieved(self, value):
        # TODO and trigger na
        return value

    def sparc_award_number(self, value):
        return NormAward(value)


class NormDatasetDescriptionFile(NormValues):

    def additional_links(self, value):
        if value.startswith('<a') and value.endswith('</a>'):
            #return ATag().asJson(value)  # TODO not ready
            at = ATag()
            j = at.asJson(value)
            #return at.href, j
            return at.href

        return value

    def link_description(self, value):
        self._error_on_na(value)
        return value

    def prior_batch_number(self, value):
        self._error_on_na(value)
        return value

    def number_of_subjects(self, value):
        try:
            return int(value)
        except ValueError as e:
            if self.addError(str(e),
                             pipeline_stage=f'{self.__class__.__name__}',
                             blame='submission',):
                logd.exception(e)

            return value  # and let the schema sort them out

    number_of_samples = number_of_subjects

    def funding(self, value):
        if 'OT' in value:
            return NormAward(value)

        seps = '|', ';', ','  # order in priority
        for sep in seps:
            if sep in value:
                out = tuple()
                # FIXME duplicate values ?!
                for funding in value.split(sep):
                    out += (funding,)

                return out

        return value.strip()

    def __contributors(self, value):
        if isinstance(value, list):
            for d in value:
                yield {self.rename_key(k):tos(nv)
                    for k, v in d.items()
                    for nv in self.normalize(k, v) if nv}
        else:
            return value

    def contributor_affiliation(self, value):
        value, _j = self._deatag(value)
        return value

    def contributor_orcid(self, value):
        # FIXME use schema
        self._error_on_na(value)
        value, _j = self._deatag(value)

        v = value.replace(' ', '').strip().rstrip()  # ah the rando tabs in a csv
        if not v:
            return
        if v.startswith('http:'):
            v = v.replace('http:', 'https:', 1)

        if not (v.startswith('ORCID:') or v.startswith('https:')):
            v = v.strip()
            if not len(v):
                return
            elif v == '0':  # FIXME ? someone using strange conventions ...
                return
            elif len(v) != 19:
                msg = f'orcid wrong length {value!r} {self._path.as_posix()!r}'
                if self.addError(idlib.Orcid._id_class.OrcidLengthError(msg)):
                    logd.error(msg)

                return

            v = 'ORCID:' + v

        else:
            if v.startswith('https:'):
                _, numeric = v.rsplit('/', 1)
            elif v.startswith('ORCID:'):
                _, numeric = v.rsplit(':', 1)

            if not len(numeric):
                return
            elif len(numeric) != 19:
                msg = f'orcid wrong length {value!r} {self._path.as_posix()!r}'
                if self.addError(idlib.Orcid._id_class.OrcidLengthError(msg)):
                    logd.error(msg)
                return

        try:
            #log.debug(f"{v} '{self.path}'")
            orcid = idlib.Orcid(v)  # XXX possible encapsulation issue
            if not orcid.identifier.checksumValid:  # FIXME should not handle this with ifs ...
                # FIXME json schema can't do this ...
                msg = f'orcid failed checksum {value!r} {self._path.as_posix()!r}'
                if self.addError(idlib.Orcid._id_class.OrcidChecksumError(msg)):
                    logd.error(msg)
                return

            yield orcid

        except (OntId.BadCurieError, idlib.Orcid._id_class.OrcidMalformedError) as e:
            msg = f'orcid malformed {value!r} {self._path.as_posix()!r}'
            if self.addError(idlib.Orcid._id_class.OrcidMalformedError(msg)):
                logd.error(msg)
            yield value

    def contributor_role(self, value):
        # FIXME normalizing here momentarily to squash annoying errors
        cell_error = ''
        def echeck(original):
            orig = original.strip()
            try:
                return NormContributorRole(orig)
            except exc.CouldNotNormalizeError as e:
                #self.addError(e,
                              #pipeline_stage=f'{self.__class__.__name__}.contributor_role',
                              #blame='submission',
                              #path=self._path)
                emsg = f'Bad value: "{orig}"'
                nonlocal cell_error
                if cell_error:
                    cell_error = cell_error + ', ' + emsg
                else:
                    cell_error = emsg

                return orig

        def elist(vl):
            rv = tuple(sorted([o for o in set(echeck(e) for e in vl
                                              # have to filter here ourselves
                                              # since clean didn't function ...
                                              # unless ... we add an expand lists step ...
                                              # HRM, interesting
                                              if e is not None and e != '')
                               if o]))
            nonlocal cell_error
            if cell_error:
                rv = tuple(exc.TabularCellError(cell_error, value=v) for v in rv)

            return rv

        if is_list_or_tuple(value):
            yield from elist(value)

        else:
            seps = '|', ';', ','  # order in priority
            for sep in seps:
                if sep in value:
                    break

            lst = value.split(sep)
            yield from elist(lst)

    def is_contact_person(self, value):
        # no truthy values only True itself
        yield value is True or isinstance(value, str) and value.lower() == 'yes'

    def title_for_complete_data_set(self, value):
        self._error_on_na(value)
        return value

    @staticmethod
    def _protocol_url_or_doi(value):
        doi = False
        if 'doi' in value:
            doi = True
        elif value.startswith('10.'):
            value = 'doi:' + value
            doi = True

        if doi:
            value = idlib.Doi(value)  # XXX possible encapsulation issue
        else:
            value = idlib.Pio(value)  # XXX possible encapsulation issue

        return value

    def protocol_url_or_doi(self, value):
        self._error_on_na(value)
        value, _j = self._deatag(value)

        for val in value.split(','):
            v = val.strip()
            if v:
                try:
                    # XXX SIGH staticmethod and inheritance :/
                    yield NormDatasetDescriptionFile._protocol_url_or_doi(v)
                except Exception as e:
                    #yield f'ERROR VALUE: {value}'  # FIXME not sure if this is a good idea ...
                    # it is not ...
                    _ps = f'{self.__class__.__name__}.protocol_url_or_doi'
                    if self.addError(str(e), pipeline_stage=_ps):
                        logd.error(e)

                    _pp = self._path.as_posix()
                    if self.addError(_pp,
                                     pipeline_stage=_ps,
                                     blame='debug'):
                        logd.critical(_pp)
                    # TODO raise exc.BadDataError from e

    def originating_article_doi(self, value):
        self._error_on_na(value)
        #self._error_on_tbd(value)  # TODO?
        value, _j = self._deatag(value)

        for val in value.split(','):
            v = val.strip()
            if v:
                try:
                    yield idlib.Doi(v)  # XXX possible encapsulation issue
                except idlib.exc.MalformedIdentifierError as e:
                    logd.exception(e)
                #doi = idlib.Doi(v)
                #if doi.valid:
                    # TODO make sure they resolve as well
                    # probably worth implementing this as part of OntId
                    #yield doi

    def keywords(self, value):
        if ';' in value:
            # FIXME error for this
            values = [v.strip() for v in value.split(';') if v]
        elif ',' in value:
            # FIXME error for this
            values = [v.strip() for v in value.split(',') if v]
        else:
            values = value,

        yield from values


class NormSubjectsFile(NormValues):

    _protocol_url_or_doi = NormDatasetDescriptionFile._protocol_url_or_doi
    protocol_url_or_doi = NormDatasetDescriptionFile.protocol_url_or_doi

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not hasattr(self.__class__, 'pyru'):
            from pysercomb.pyr import units as pyru
            self.__class__.pyru = pyru

    def __subject_id(self, value):
        # XXX unused, ids like this need to be fixed in the source data
        if not isinstance(value, str):
            msg = f'Bad type for subject_id: {type(value)}'
            if self.addError(msg,
                             pipeline_stage=self.__class__.__name__,
                             blame='submission',):
                logd.error(msg)

            return str(value)
        else:
            return value

    def software_url(self, value):
        value, _j = self._deatag(value)
        return value

    def software_rrid(self, value):
        yield from self._rrid(value)

    def species(self, value):
        nv = NormSpecies(value)
        #yield self._query(nv, 'NCBITaxon')
        return nv

    def strain(self, value):
        if value == 'DSH':
            value = 'domestic shorthair'

        return value
        #wat = self._query(value, 'BIRNLEX')  # FIXME
        #yield wat

    #sex = NormSex
    def sex(self, value):
        nv = NormSex(value)
        # reminder we don't do ontlogy lookups during normalization
        # because it requires network access
        #yield self._query(nv, 'PATO')
        return nv

    def gender(self, value):
        # FIXME gender -> sex for animals, requires two pass normalization ...
        return self.sex(value)

    def group(self, value):
        # trigger n/a
        return value

    def pool_id(self, value):
        # trigger n/a
        return value

    def handedness(self, value):
        # needed to tirgger n/a fixes I think
        # TODO
        return value

    def _param(self, value):
        self._error_on_na(value)

        if isinstance(value, numbers.Number):
            yield self.pyru.ur.Quantity(value)
            return

        try:
            pv = self.pyru.UnitsParser(value).asPython()
        except self.pyru.UnitsParser.ParseFailure as e:
            caller_name = e.__traceback__.tb_frame.f_back.f_code.co_name
            msg = f'Unexpected and unhandled value "{value}" for {caller_name}'
            if self.addError(msg, pipeline_stage=self.__class__.__name__, blame='pipeline'):
                log.error(msg)

            if value.strip().lower() in ('unknown', 'uknown'):
                yield UNKNOWN
            else:
                yield value

            return

        #if not pv[0] == 'param:parse-failure':
        if pv is not None:  # parser failure  # FIXME check on this ...
            yield pv  # this one needs to be a string since it is combined below
        else:
            # TODO warn
            yield value

    def _param_unit(self, value, unit):
        if value.strip().lower() in ('unknown', 'uknown'):
            yield UNKNOWN
        else:
            yield from self._param(value + unit)

    def age(self, value):
        if value in ('adult',):
            msg = (f'Bad value for age: {value}\ndid you want to put that in age_cagegory instead?\n'
                   f'"{self._path}"')
            logd.error(msg)
            self.addError(msg, pipeline_stage=self.__class__.__name__, blame='submission',
                          path=self._path)
            return value

        yield from self._param(value)

    def age_years(self, value):
        # FIXME the proper way to do this is to detect
        # the units and lower them to the data, and leave the aspect
        yield from self._param_unit(value, 'years')

    #def age_category(self, value):
        #yield self._query(value, 'UBERON')

    def experimental_log_file_name(self, value):
        return value

    def age_range_min(self, value):
        yield from self._param(value)

    def age_range_max(self, value):
        if value in ('Normal',):
            msg = (f'Bad value for age_range_max: {value}\n'
                   'did you want to put that in age_cagegory instead?\n'
                   f'"{self._path}"')
            if self.addError(msg, pipeline_stage=self.__class__.__name__, blame='submission',
                             path=self._path):
                logd.error(msg)
            return value

        yield from self._param(value)

    age_range_max_disease = age_range_max # FIXME pretty sure these are a bad merge?

    def mass(self, value):
        yield from self._param(value)

    body_mass = mass

    def weight(self, value):
        yield from self._param(value)

    def weight_kg(self, value):  # TODO populate this?
        yield from self._param_unit(value, 'kg')

    def height_inches(self, value):
        yield from self._param_unit(value, 'in')

    def rrid_for_strain(self, value):
        yield from self._rrid(value)

    def _rrid(self, value):
        yield rrid(value, self)
        # OF COURSE RRIDS ARE SPECIAL

    #def protocol_io_location(self, value):  # FIXME need to normalize this with dataset_description
        #yield value

    def _process_dict(self, dict_):
        """ deal with multiple fields """
        out = {k:v for k, v in dict_.items() if k not in self.skip}
        for h_unit, h_value in zip(self.h_unit, self.h_value):
            if h_value not in dict_:  # we drop null cells so if one of these was null then we have to skip it here too
                continue

            dhv = dict_[h_value]
            if isinstance(dhv, str):
                try:
                    dhv = ast.literal_eval(dhv)
                except ValueError as e:
                    raise exc.UnhandledTypeError(f'{h_value} {dhv!r} was not parsed!') from e

            compose = dhv * self.pyru.ur.parse_units(dict_[h_unit])
            #_, v, rest = parameter_expression(compose)
            #out[h_value] = str(UnitsParser(compose).for_text)  # FIXME sparc repr
            #breakpoint()
            out[h_value] = compose #UnitsParser(compose).asPython()

        if 'gender' in out and 'species' in out:
            if out['species'] != OntTerm('NCBITaxon:9606'):
                out['sex'] = out.pop('gender')

        return out

    #def experiment_date(self):
        #pass

    def ___iter__(self):
        """ this is still used """
        if self._is_json:
            yield from (self._process_dict({k:nv for k, v in d.items()
                                           for nv in self.normalize(k, v) if nv})
                        for d in self._data_raw)

        else:
            yield from (self._process_dict({k:nv for k, v in zip(r._fields, r) if v
                                           and k not in self.skip_cols
                                           for nv in self.normalize(k, v) if nv})
                        for r in self.bc.rows)

    def triples_gen(self, prefix_func):
        """ NOTE the subject is LOCAL """


class NormSamplesFile(NormSubjectsFile):

    def specimen_anatomical_location(self, value):
        seps = '|', ';'
        for sep in seps:
            if sep in value:
                for v in value.split(sep):
                    v = v.strip()
                    if v:
                        yield v
                return

        else:
            yield value.strip()

    def __sample_id(self, value):
        # XXX unused, ids like this need to be fixed in the source data
        if not isinstance(value, str):
            msg = f'Bad type for sample_id: {type(value)}'
            if self.addError(msg,
                             pipeline_stage=self.__class__.__name__,
                             blame='submission',):
                logd.error(msg)

            return str(value)
        else:
            return value


class NormPerformancesFile(NormSubjectsFile):
    """ this will do for now """
