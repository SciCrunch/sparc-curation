class DatasetStructureH(PathData, dat.DatasetStructure):
    """ a homogenous representation """

    @property
    def path(self):
        return self

    @property
    def is_organization(self):
        if self.anchor is not None:
            if self == self.anchor:
                return True

            return self.id.startswith('N:organization:')

    @property
    def organization(self):
        """ organization represents a permissioning boundary
            for blackfynn, so above this we would have to know
            in advance the id and have api keys for it and the
            containing folder would have some non-blackfynn notes
            also it seems likely that the same data could appear in
            multiple different orgs, so that would mean linking locally
        """

        # FIXME in reality files can have more than one org ...
        if self.is_organization:
            return self
        elif self.parent:
            return self.parent.organization

    @property
    def is_dataset(self):
        return self.id.startswith('N:dataset:')

    @property
    def dataset(self):
        if self.is_dataset:
            return self
        elif self.parent:
            return self.parent.dataset

    @property
    def id(self):
        if not hasattr(self, '_id'):
            if self._meta is not None:
                self._id = self._meta.id
            else:
                self._id = None
                # give that fthing should be sitting atop already fetched data, this seems ok
                # we can kill these more frequently anyway? they live for the time it takes them
                # to produce their data and then they move on?

        return self._id

    @property
    def dataset_name_proper(self):
        try:
            award = next(iter(set(self.award)))  # FIXME len1? testing?
        except StopIteration:
            award = '?-no-award-?'

        pis = list(self.PI)
        if not pis:
            pis = '?-no-pi-?',
        PI = ' '.join(pis)

        species = list(self.species)
        if not species:
            species = '?-no-species-?',
        species = ' '.join(s.label if isinstance(s, OntTerm) else s for s in species)

        return f'{award} {PI} {species} {self.organ} {self.modality}'

    @property
    def bf_size(self):
        size = self._meta.size
        if size:
            return size
        elif self.path.is_dir():
            size = 0
            for path in self.path.rglob('*'):
                if path.is_file():
                    try:
                        size += path.cache.meta.size
                    except OSError as e:
                        log.warning(f'No cached file size. Assuming it is not tracked. {path}')

            return size

        else:
            log.warning(f'unknown thing at path {self.path}')

    @property
    def _submission_objects(self):
        for p in self.submission_paths:
            try:
                miss = dat.SubmissionFile(p)
                if miss.data:
                    yield miss
            except exc.NoDataError as e:
                self._errors.append(e)  # NOTE we treat empty file as no file
            except AttributeError as e:
                log.warning(f'unhandled metadata type {e!r}')
                self._errors.append(e)

    @property
    def submission(self):
        if not hasattr(self, '_subm_cache'):
            self._subm_cache = list(self._submission_objects)

        yield from self._subm_cache

    @property
    def _dd(self):
        for p in self.dataset_description_paths:
            yield dat.DatasetDescriptionFile(p)

    @property
    def _dataset_description_objects(self):
        for p in self.dataset_description_paths:
            #yield from DatasetDescription(t)
            # TODO export adapters for this ... how to recombine and reuse ...
            try:
                dd = dat.DatasetDescriptionFile(p)
                if dd.data:
                    yield dd
            except exc.NoDataError as e:
                self._errors.append(e)  # NOTE we treat empty file as no file
            except AttributeError as e:
                log.warning(f'unhandled metadata type {e!r}')
                self._errors.append(e)

    @property
    def dataset_description(self):
        if not hasattr(self, '_dd_cache'):
            self._dd_cache = list(self._dataset_description_objects)

        yield from self._dd_cache

    @property
    def _subjects_objects(self):
        """ really subjects_file """
        for path in self.subjects_paths:
            try:
                sf = dat.SubjectsFile(path)
                if sf.data:
                    yield sf
            except exc.NoDataError as e:
                self._errors.append(e)  # NOTE we treat empty file as no file
            except AttributeError as e:
                log.warning(f'unhandled metadata type {e!r}')
                self._errors.append(e)

    @property
    def _samples_objects(self):
        """ really samples_file """
        for path in self.samples_paths:
            try:
                sf = dat.SamplesFile(path)
                if sf.data:
                    yield sf
            except exc.NoDataError as e:
                self._errors.append(e)  # NOTE we treat empty file as no file
            except AttributeError as e:
                log.warning(f'unhandled metadata type {e!r}')
                self._errors.append(e)

    @property
    def subjects(self):
        if not hasattr(self, '_subj_cache'):
            self._subj_cache = list(self._subjects_objects)

        yield from self._subj_cache

    @property
    def samples(self):
        if not hasattr(self, '_samp_cache'):
            self._samp_cache = list(self._samples_objects)

        yield from self._samp_cache

    @property
    def data_lift(self):
        """ used to validate repo structure """

        out = {}

        for section_name in ('submission', 'dataset_description', 'subjects'):
            #path_prop = section_name + '_paths'
            for section in getattr(self, section_name):
                section_name += '_file'
                tp = section.path.as_posix()
                if section_name in out:
                    ot = out[section_name]
                    # doing it this way will cause the schema to fail loudly :)
                    if isinstance(ot, str):
                        out[section_name] = [ot, tp]
                    else:
                        ot.append(tp)
                else:
                    out[section_name] = tp

        return out

    @property
    def meta_paths(self):
        """ All metadata related paths. """
        #yield self.path
        yield from self.submission_paths
        yield from self.dataset_description_paths
        yield from self.subjects_paths

    @property
    def _meta_tables(self):
        yield from (s.t for s in self.submission)
        yield from (dd.t for dd in self.dataset_description)
        yield from (s.t for s in self.subjects)

    @property
    def meta_sections(self):
        """ All metadata related objects. """
        yield self
        yield from self.submission
        yield from self.dataset_description
        yield from self.subjects

    @property
    def __errors(self):  # XXX deprecated
        gen = self.meta_sections
        next(gen)  # skip self
        for thing in gen:
            yield from thing.errors

        yield from self._errors

    @property
    def report(self):

        r = '\n'.join([f'{s:>20}{l:>20}' for s, l in zip(self.status.report, self.lax.report)])
        return self.name + f"\n'{self.id}'\n\n" + r


    def _with_errors(self, ovd, schema):
        # FIXME this needs to be split into in and out
        # and really all this section should do is add the errors
        # on failure and it should to it in place in the pipeline

        # data in -> check -> with errors -> normalize/correct/map ->
        # -> augment and transform -> data out check -> out with errors

        # this flow splits at normalize/correct/map -> export back to user

        ok, valid, data = ovd
        out = {}  # {'raw':{}, 'normalized':{},}
        # use the schema to auto fill things that we are responsible for
        # FIXME possibly separate this to our own step?
        try:
            if 'id' in schema.schema['required']:
                out['id'] = self.id
        except KeyError:
            pass

        if not ok:
            # FIXME this will dump the whole schema, go smaller
            if 'errors' not in out:
                out['errors'] = []

            #out['errors'] += [{k:v if k != 'schema' else k
                               #for k, v in e._contents().items()}
                              #for e in data.errors]
            out['errors'] += valid.json()

        if schema == self.schema:  # FIXME icky hack
            for section_name, path in data.items():
                if section_name == 'id':
                    # FIXME MORE ICK
                    continue
                # output sections don't all have python representations at the moment
                if hasattr(self, section_name):
                    section = next(getattr(self, section_name))  # FIXME non-homogenous
                    # we can safely call next here because missing or multi
                    # sections will be kicked out
                    # TODO n-ary cases may need to be handled separately
                    sec_data = section.data_with_errors
                    #out.update(sec_data)  # the magic of being self describing
                    out[section_name] = sec_data  # FIXME

        else:
            out['errors'] += data.pop('errors', [])

        if self.is_organization:  # FIXME ICK
            # can't run update in cases where there might be bad data
            # in the original data
            out.update(data)

        return out

    @property
    def data_with_errors(self):
        # FIXME remove this
        # this still ok, dwe is very simple for this case
        return self._with_errors(self.schema.validate(copy.deepcopy(self.data)), self.schema)

    def dump_all(self):
        return {attr: express_or_return(getattr(self, attr))
                for attr in dir(self) if not attr.startswith('_')}

    def __eq__(self, other):
        # TODO order by status
        if self.path is self:
            return super().__eq__(other.path)

        return self.path == other.path

    def __gt__(self, other):
        if self.path is self:
            return super().__gt__(other.path)

        return self.path > other.path

    def __lt__(self, other):
        if self.path is self:
            return super().__lt__(other.path)

        return self.path < other.path

    def __hash__(self):
        return hash((hash(self.__class__), self.id))  # FIXME checksum? (expensive!)

    def __repr__(self):
        return f'{self.__class__.__name__}(\'{self.path}\')'


class LThing:
    def __init__(self, lst):
        self._lst = lst


def express_or_return(thing):
    return list(thing) if isinstance(thing, GeneratorType) else thing
