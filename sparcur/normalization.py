""" string normalizers, strings that change their content to match a standard """

from html.parser import HTMLParser
from sparcur.core import log, HasErrors


class NormSimple(str):
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
    }

class NormSex(NormSimple):
    data = {
        'm':'male',
        'f':'female',
    }


class NormHeader(NormSimple):
    armi = 'age_range_min'
    arma = 'age_range_max'
    data = {
        'age_range_minimum': armi,
        'age_range_maximum': arma,
        'protocol_io_location': 'protocol_url_or_doi',
    }


class NormContributorRole(str):
    values = ('ContactPerson',
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

    @staticmethod
    def levenshteinDistance(s1, s2):
        if len(s1) > len(s2):
            s1, s2 = s2, s1

        distances = range(len(s1) + 1)
        for i2, c2 in enumerate(s2):
            distances_ = [i2+1]
            for i1, c1 in enumerate(s1):
                if c1 == c2:
                    distances_.append(distances[i1])
                else:
                    distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
            distances = distances_
        return distances[-1]

    @classmethod
    def normalize(cls, value):
        # a hilariously slow way to do this
        # also not really normalization ... more, best guess for what people were shooting for
        if value:
            best = sorted((cls.levenshteinDistance(value, v), v) for v in cls.values)[0]
            distance = best[0]
            normalized = best[1]
            cutoff = len(value) / 2
            if distance > cutoff:
                normalized = (f'ERROR VALUE: "{value}" could not be normalized, best was {normalized} '
                              f'with distance {distance} cutoff was {cutoff}')

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

    def __init__(self, obj_inst):
        super().__init__()
        self._obj_inst = obj_inst

    def _bind(self):
        self._record_type_key_header = self._obj_inst.record_type_key_header
        self._norm_to_orig_alt = self._obj_inst.norm_to_orig_alt
        self._norm_to_orig_header = self._obj_inst.norm_to_orig_header
        self._groups_alt = self._obj_inst.groups_alt
        self._path = self._obj_inst.path

    def _deatag(self, value):
        if value.startswith('<a') and value.endswith('</a>'):
            at = ATag()
            j = at.asJson(value)
            return at.href, j

        return value, None

    @staticmethod
    def _query(value, prefix):
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

    def _normv(self, thing, key=None, path=tuple()):
        #print(path)
        if isinstance(thing, dict):
            out = {}
            for k, v in thing.items():
                if k == self._record_type_key_header:
                    out[k] = v
                elif k in self._norm_to_orig_header:
                    out[k] = self._normv(v, key, path)
                elif k in self._norm_to_orig_alt:
                    out[k] = self._normv(v, k, path + (k,))
                elif k in self._groups_alt:
                    out[k] = self._normv(v, k, path + (k,))
                else:
                    raise ValueError(f'what is going on here?! {k} {v}')

            return out

        else:
            # TODO make use of path
            # FIXME I do NOT like this pattern :/

            if isinstance(key, str) and hasattr(self, key):
                out = getattr(self, key)(thing)
                if isinstance(out, GeneratorType):
                    out = tuple(out)
                    if len(out) == 1:  # FIXME find the actual source of double packing
                        out = out[0]
                    elif not out:
                        msg = f'Normalization {key} returned None for input {thing}'
                        self.addError(msg,
                                      pipeline_stage=f'{self.__class__.__name__}.{key}',
                                      logfunc=log.critical,
                                      blame='pipeline',)
                        out = None

                return out

            return thing

    @property
    def data(self):
        nk = self._obj_inst._normalize_keys()
        self._bind()
        data = self._normv(nk)
        self.embedErrors(data)
        return data



class NormSubmissionFile(NormValues):

    def sparc_award_number(self, value):
        return nml.NormAward(value)


class NormDatasetDescriptionFile(NormValues):

    def additional_links(self, value):
        if value.startswith('<a') and value.endswith('</a>'):
            return ATag().asJson(value)

        return value

    def _metadata_version_do_not_change(self, value):
        return 'lolololol'

    def contributors(self, value):
        if isinstance(value, list):
            for d in value:
                yield {self.rename_key(k):tos(nv)
                    for k, v in d.items()
                    for nv in self.normalize(k, v) if nv}
        else:
            return value

    def contributor_orcid_id(self, value):
        # FIXME use schema
        v = value.replace(' ', '')
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
                self.addError(OrcidId.OrcidLengthError(msg))
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
                self.addError(OrcidId.OrcidLengthError(msg))
                logd.error(msg)
                return

        try:
            #log.debug(f"{v} '{self.path}'")
            orcid = OrcidId(v)
            if not orcid.checksumValid:
                # FIXME json schema can't do this ...
                msg = f'orcid failed checksum {value!r} {self._path.as_posix()!r}'
                self.addError(OrcidId.OrcidChecksumError(msg))
                logd.error(msg)
                return

            yield orcid

        except (OntId.BadCurieError, OrcidId.OrcidMalformedError) as e:
            msg = f'orcid malformed {value!r} {self._path.as_posix()!r}'
            self.addError(OrcidId.OrcidMalformedError(msg))
            logd.error(msg)
            yield value

    def contributor_role(self, value):
        # FIXME normalizing here momentarily to squash annoying errors
        def echeck(value, original):
            if value.startswith('ERROR VALUE:'):
                self.addError(value.split(':', 1)[-1].strip(),
                              pipeline_stage=f'{self.__class__.__name__}.contributor_role',
                              logfunc=logd.error)
                return original
            else:
                return value

        if isinstance(value, list):
            yield tuple(sorted(set(echeck(nml.NormContributorRole(e.strip()), e) for e in value)))
        else:
            yield tuple(sorted(set(echeck(nml.NormContributorRole(e.strip()), e) for e in value.split(','))))

    def is_contact_person(self, value):
        # no truthy values only True itself
        yield value is True or isinstance(value, str) and value.lower() == 'yes'

    def _protocol_url_or_doi(self, value):
        doi = False
        if 'doi' in value:
            doi = True
        elif value.startswith('10.'):
            value = 'doi:' + value
            doi = True

        if doi:
            value = DoiId(value)
        else:
            value = PioId(value).normalize()

        return value

    def protocol_url_or_doi(self, value):
        value, _j = self._deatag(value)

        for val in value.split(','):
            v = val.strip()
            if v:
                try:
                    yield  self._protocol_url_or_doi(v)
                except BaseException as e:
                    yield f'ERROR VALUE: {value}'  # FIXME not sure if this is a good idea ...
                    self.addError(e,
                                  pipeline_stage=f'{self.__class__.__name__}.protocol_url_or_doi',
                                  logfunc=logd.error)
                    self.addError(self._path.as_posix(),
                                  pipeline_stage=f'{self.__class__.__name__}.protocol_url_or_doi',
                                  logfunc=logd.critical,
                                  blame='debug')
                    # TODO raise exc.BadDataError from e

    def originating_article_doi(self, value):
        value, _j = self._deatag(value)

        for val in value.split(','):
            v = val.strip()
            if v:
                doi = DoiId(v)
                if doi.valid:
                    # TODO make sure they resolve as well
                    # probably worth implementing this as part of OntId
                    yield doi

    def keywords(self, value):
        if ';' in value:
            # FIXME error for this
            values = [v.strip() for v in value.split(';') if v]
        elif ',' in value:
            # FIXME error for this
            values = [v.strip() for v in value.split(',') if v]
        else:
            values = value,

        return values
        #log.debug(f'{values}')
        #for value in values:
            #match = self._query(value, prefix=None)
            #if match and False:  # this is incredibly broken at the moment
                #yield match
            #else:
                #yield value


class NormSubjectsFile(NormValues):
    def __data(self):
        # sigh
        #notunique = (len([r for r in self.bc.subject_id if r]) !=
                     #len([k for k in self.bc._byCol__indexes['subject_id']
                          #if k != 'subject_id' and k]))
        if self._is_json:
            # FIXME ... only if it is a json list ???
            sids = Counter(r['subject_id'] for r in self._data_raw if r)
            values = self._data_raw
        elif not hasattr(self.bc, 'subject_id'):
            sids = {}
            values = list(self)
            msg = f'subject_id column missing! for {self.path}'
            logd.critical(msg)
            self.addError(msg)
        else:
            sids = Counter(r for r in self.bc.subject_id if r)
            values = list(self)

        notunique = {s:c for s, c in sids.items() if c > 1}

        if notunique:
            msg = f'subject_ids are not unique for {self.path}\n{notunique}'
            logd.critical(msg)
            self.addError(msg)

        out = {self.dict_key: values}
        if not self._is_json:
            for k, heads in self.horizontals.items():
                # TODO make sure we actually check that the horizontal
                # isn't used by someone else already ... shouldn't be
                # it should be skipped but maybe not?
                tups = sorted(set(_ for _ in zip(*(getattr(self.bc, head, [])
                                                # FIXME one [] drops whole horiz group ...
                                                for head in heads))
                                if any(_)))
                if tups:
                    out[k] = [{k:v for k, v in zip(heads, t) if v} for t in tups]

        self.embedErrors(out)
        return out

    def ___init__(self, path):
        super().__init__(path)
        if self._is_json:
            header = [d.keys() for d in self._data_raw]  # FIXME max may have wrong top level schema
        else:
            header = self.bc.header

        # units merging
        # TODO pull the units in the parens out
        self.h_unit = [k for k in header if '_unit' in k]
        h_value = [k.replace('_units', '').replace('_unit', '') for k in self.h_unit]
        no_unit = [k for k in header if '_unit' not in k]
        #self.h_value = [k for k in self.bc.header if '_units' not in k and any(k.startswith(hv) for hv in h_value)]
        self.h_value = [k for hv in h_value
                        for k in no_unit
                        if k.startswith(hv) or k.endswith(hv)]
        err = f'Problem! {self.h_unit} {self.h_value} {header} \'{self.path}\''
        #assert all(v in header for v in self.h_value), err
        assert len(self.h_unit) == len(self.h_value), err
        self.skip = self.h_unit + self.h_value

        self.skip_cols += tuple(set(_ for v in self.horizontals.values() for _ in v))

    def species(self, value):
        nv = nml.NormSpecies(value)
        #yield self._query(nv, 'NCBITaxon')
        return nv

    def strain(self, value):
        if value == 'DSH':
            value = 'domestic shorthair'

        return value
        #wat = self._query(value, 'BIRNLEX')  # FIXME
        #yield wat

    def sex(self, value):
        nv = nml.NormSex(value)
        #yield self._query(nv, 'PATO')
        return nv

    def gender(self, value):
        # FIXME gender -> sex for animals, requires two pass normalization ...
        yield from self.sex(value)

    def _param(self, value):
        if isinstance(value, numbers.Number):
            return pyru.ur.Quantity(value)

        try:
            pv = pyru.UnitsParser(value).asPython()
        except pyru.UnitsParser.ParseFailure as e:
            caller_name = e.__traceback__.tb_frame.f_back.f_code.co_name
            msg = f'Unexpected and unhandled value {value} for {caller_name}'
            log.error(msg)
            self.addError(msg, pipeline_stage=self.__class__.__name__ + '.curation-error')
            return value

        #if not pv[0] == 'param:parse-failure':
        if pv is not None:  # parser failure  # FIXME check on this ...
            yield pv  # this one needs to be a string since it is combined below
        else:
            # TODO warn
            yield value

    def _param_unit(self, value, unit):
        yield from self._param(value + unit)

    def age(self, value):
        yield from self._param(value)

    def age_years(self, value):
        # FIXME the proper way to do this is to detect
        # the units and lower them to the data, and leave the aspect
        yield from self._param_unit(value, 'years')

    #def age_category(self, value):
        #yield self._query(value, 'UBERON')

    def age_range_min(self, value):
        yield from self._param(value)

    def age_range_max(self, value):
        yield from self._param(value)

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
        yield value

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

            compose = dhv * pyru.ur.parse_units(dict_[h_unit])
            #_, v, rest = parameter_expression(compose)
            #out[h_value] = str(UnitsParser(compose).for_text)  # FIXME sparc repr
            #breakpoint()
            out[h_value] = compose #UnitsParser(compose).asPython()

        if 'gender' in out and 'species' in out:
            if out['species'] != OntTerm('NCBITaxon:9606'):
                out['sex'] = out.pop('gender')

        return out

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
    def __data(self):
        #notunique = (len([r for r in self.bc.sample_id if r]) !=
                     #len([k for k in self.bc._byCol__indexes['sample_id']
                          #if k != 'sample_id' and k]))

        if self._is_json:
            sids = Counter(r['sample_id'] for r in self._data_raw if r)
        elif not hasattr(self.bc, 'sample_id'):
            sids = {}
            values = list(self)
            msg = f'sample_id column missing! for {self.path}'
            logd.critical(msg)
            self.addError(msg)
        else:
            sids = Counter(r for r in self.bc.sample_id if r)

        notunique = {s:c for s, c in sids.items() if c > 1}
        values = list(self)
        if notunique:
            if not values or 'sample_id' not in values[0]:
                # [] [{}], caused by a bunch of N/A or missing rows
                breakpoint()

            msg = f'sample_ids are not unique for {self.path}\n{notunique}'
            logd.critical(msg)
            self.addError(msg)
            # FIXME this needs to be pipelined so we can do the diff ??
            for i, v in enumerate(values):
                v['local_sample_id'] = v['sample_id']
                if 'subject_id' in v:
                    v['sample_id'] = v['subject_id'] + '-' + v['sample_id']  # FIXME '/' gets quoted ...
                else:
                    v['sample_id'] = f'ERROR-{i}-' + v['sample_id']

        out = {self.dict_key: values}
        for k, heads in self.horizontals.items():
            # TODO make sure we actually check that the horizontal
            # isn't used by someone else already ... shouldn't be
            # it should be skipped but maybe not?
            tups = sorted(set(_ for _ in zip(*(getattr(self.bc, head, [])
                                               # FIXME one [] drops whole horiz group ...
                                               for head in heads))
                              if any(_)))
            if tups:
                out[k] = [{k:v for k, v in zip(heads, t) if v} for t in tups]

        self.embedErrors(out)
        return out

    def specimen_anatomical_location(self, value):
        seps = '|', ';'
        #skip = (
            #'UBERON:0000465',  # material anatomical entity
        #)
        for sep in seps:
            if sep in value:
                for v in value.split(sep):
                    v = v.strip()
                    if v:
                        yield v
                        #out = self._query(v, 'UBERON')
                        #if out and out.curie not in skip:
                            #yield out
                        #else:
                            #yield v

                return

        else:
            yield value.strip()
            #out = self._query(value, 'UBERON')
            #if out and out.curie not in skip:
                #yield out
            #else:
                #yield value


