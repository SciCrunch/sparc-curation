import copy
from typing import Tuple
from functools import wraps
from collections import defaultdict
import idlib
from sparcur import schemas as sc
from sparcur import exceptions as exc
from sparcur import normalization as nml
from sparcur.core import log, logd, JPointer, HasErrors
from sparcur.utils import register_type


def collect(*oops, unpacked=True):
    def decorator(generator_function):
        @wraps(generator_function)
        def inner(*args, **kwargs):
            out = tuple(generator_function(*args, **kwargs))
            if unpacked:
                return out
            else:
                return out,

        return inner

    if oops:
        generator_function, = oops
        return decorator(generator_function)
    else:
        return decorator


class Derives:
    """ all derives must return as a tuple, even if they are length 1 """

    @staticmethod
    def contributor_name(name) -> Tuple[str, str]: 
        if ', ' in name:
            last, first = name.split(', ', 1)
        elif ',' in name:
            last, first = name.split(',', 1)  # the error will be recorded by the json schema
        elif ' ' in name:
            first, last = name.split(' ', 1)
        else:
            first = 'ERROR IN NAME see lastName for content'
            last = name

        return first, last

    @staticmethod
    @collect
    def creators(contributors):
        for c in contributors:
            if 'contributor_role' in c and 'Creator' in c['contributor_role']:
                # FIXME diry diry mutation here that should happen in a documente way
                cont = {**c}  # instead of importing copy since this is a one deep
                cont.pop('contributor_role', None)
                cont.pop('is_contact_person', None)
                yield cont

    @staticmethod
    def award_number(raw_award_number, funding) -> str:
        return nml.NormAward(nml.NormAward(raw_award_number))

    @staticmethod
    def pi(contributors):
        pointers = [JPointer(f'/contributors/{i}')
                    for i, c in enumerate(contributors)
                    if (('PrincipalInvestigator' in c['contributor_role'])
                        if 'contributor_role' in c else False)]

        if len(pointers) == 1:
            pointer, = pointers
            return pointer

        return pointers

    @staticmethod
    def dataset_species(subjects) -> Tuple[tuple]:
        out = set()
        for subject in subjects:
            if 'species' in subject:
                out.add(subject['species'])

        if len(out) == 1:
            return next(iter(out))

        return tuple(out)

    @staticmethod
    def doi(doi_string):  # FIXME massive network sandbox violation here
        """ check if a doi string resolves, if it does, return it """
        if doi_string is None:
            raise TypeError('WHAT HAVE YOU DONE!?')

        doi = idlib.Doi(doi_string)
        try:
            metadata = doi.metadata()  # FIXME network sandbox violation
            if metadata is not None:
                return doi
        except idlib.exceptions.RemoteError:
            # sometimes a doi is present on the platform but does not resolve
            # in which case we don't add it as metadata because it has not
            # been officially published, just reserved, this check is more
            # correct than checkin the status on the platform
            # FIXME HOWEVER it violates the network sandbox, so we probably
            # need an extra step during the data retrieval phase which attempts
            # to fetch all the doi metadata
            pass
        except Exception as e:
            # XXX random errors need to be ignored here for now
            # since this really should not be run at this step
            # due to the network dependency, we need a post-network
            # step where we can strip out all the things that fail
            log.exception(e)

    @staticmethod
    def _lift_mr(path_dataset, dataset_relative_path, record, should_log):
        parent = dataset_relative_path.parent
        if 'filename' not in record:
            msg = f'filename missing from record in {dataset_relative_path}'
            raise exc.BadDataError(msg)

        record_drp = parent / record['filename']

        # FIXME this is validate move elsewhere ??? but this is also
        # where we want to embed data about the path ...
        # but I supposed we could inject errors later ? this is an
        # extremely obscure place to inject these ...
        # and to do that we need access to the path
        # and it is clear given that we passed in THIS_PATH

        # FIXME TODO how to deal with sparse datasets
        _record_path = path_dataset / record_drp  # do not include in export

        _cache = _record_path.cache
        if _cache is None:
            lifted = _record_path._jsonMetadata()  # will produce the error for us
        else:
            lifted = _cache._jsonMetadata()

        if 'errors' in lifted:
            he = HasErrors(pipeline_stage='Derives._lift_mr')
            _message = lifted['errors'].pop()['message']
            # FIXME pretty sure that path in addError is used for both
            # the json path and the file system path
            message = ('Non-existent path listed in manifest '
                       f'{dataset_relative_path}\n{_message}')
            if he.addError(message,
                           blame='submission',
                           path=_record_path):
                if should_log:
                    should_log = False
                    logd.error(message)

            he.embedErrors(lifted)

        lifted['prov:wasDerivedFrom'] = (
            # have to reattach path_dataset because Path.cwd() may not be
            # the dataset root (usually the organizaiton root)
            path_dataset / dataset_relative_path).cache_identifier
        lifted['dataset_relative_path'] = record_drp
        lifted['manifest_record'] = {
            k:v for k, v in record.items() if k != 'filetype'
        }
        if 'additional_types' in record:
            # FIXME TODO make sure that the mimetypes match
            # FIXME currently a string, either change name or make a list?
            lifted['mimetype'] = record['additional_types']

        return lifted, should_log

    @staticmethod
    def _scaffolds(lifted, scaffolds):
        # FIXME this is burried here, should be more like how we
        # handle the xml extraction

        expected = 'organ', 'species'
        if ('mimetype' in lifted and
            lifted['mimetype'] in ('inode/vnd.abi.scaffold+directory',
                                   'inode/x.vnd.abi.scaffold+directory',)):
            # TODO type -> scaffold
            # TODO look for additional metadata from interior manifest
            scaf = copy.deepcopy(lifted)
            # FIXME should be a move step for organ and specie
            for key in expected:
                if key in scaf['manifest_record']:
                    scaf[key] = scaf['manifest_record'][key]

            scaffolds.append(scaf)

    @classmethod
    def path_metadata(cls, path_dataset, manifests, xmls):
        path_metadata = []
        scaffolds = []  # FIXME need a better abstraction for additional known types e.g. the mbf segmentations
        for manifest in manifests:
            he = HasErrors(pipeline_stage='Derives.path_metadata')
            if 'contents' in manifest:
                contents = manifest['contents']
                if 'manifest_records' in contents:
                    drp = manifest['dataset_relative_path']
                    _should_log = True
                    for record in contents['manifest_records']:
                        try:
                            lifted, _should_log = cls._lift_mr(
                                path_dataset, drp, record, _should_log)
                        except FileNotFoundError as e:
                            # FIXME all this is horribly tangled and we repeatedly compute the same value
                            record_drp = drp.parent / record['filename']
                            msg = f'A path listed in manifest {drp} does not exist! {record_drp}'
                            if he.addError(msg, blame='submission', path=drp):
                                logd.error(e)
                            continue  # FIXME need this in the errors record
                        except exc.BadDataError as e:
                            if he.addError(e, blame='submission', path=drp):
                                logd.error(e)
                            continue  # FIXME need this in the errors record
                        path_metadata.append(lifted)
                        if 'errors' not in lifted:
                            cls._scaffolds(lifted, scaffolds)

                    he.embedErrors(manifest)

        path_metadata.extend(xmls['xml'])
        # TODO try to construct/resolve the referent paths in these datasets as well
        # getting the path json metadata will embed errors about missing files for us
        return path_metadata, scaffolds

    @staticmethod
    def samples_to_subjects(samples, schema_version=None):
        """ extract subject information from samples sheet """
        # TODO only allow this for <= 1.2.3 ? or no, really
        # only the subject id should be present in later versions

        # FIXME TODO derive subject fields from schema?
        subject_fields = (
            'subject_id',
            'ear_tag_number',
            'rrid_for_strain',
            'genus',
            'species',
            'strain',
            'sex',
            'gender',
            'age',
            'age_category',
            'body_weight',
            'body_mass',
            'body_mass_weight',
            'height_inches',
            'age_years',
            'organism_rrid',
            'age_range_min',
            'age_range_max',
            'date_of_euthanasia',
        )
        subjects = [{k:v for k, v in blob_sample.items() if k in subject_fields}
                    for blob_sample in samples]
        return subjects,

    @staticmethod
    def validate_structure(path, dir_structure, performances, subjects, samples):
        he = HasErrors(pipeline_stage='Derives.validate_structure')

        # FIXME TODO handle pools as well and figure out cases where subjects/samples are metadata only

        # for dataset templates of the 1.* series
        # general approach: set of all specimen ids and set of all
        # folder names take the ones that match ignore the known ok
        # that do not, and warn on all the rest that do not match
        # and that are not inside a known specimen or subject folder

        valid_top_123 = ('source', 'primary', 'derivative',  # FIXME not here :/ schema somehow?
                         'code', 'docs', 'protocol')

        def top_level(drp):
            return drp.parent.name == '' and drp.name in valid_top_123
            
        # absolute_paths = [path / pblob['dataset_relative_path'] for pblob in dir_structure]
        dd = defaultdict(list)
        for pblob in dir_structure:
            drp = pblob['dataset_relative_path']
            p = drp.parts
            dd[p[-1]].append((len(p), drp, p[::-1]))

        dirs = {k:av for k, vs in dd.items()
                for av in ([v for v in vs if not top_level(v[1])],)
                # cull empty in a single step
                if av} 

        perfs = {p['performance_id']:p for p in performances}

        # subject_id could be missing, but we filter failures on all of
        # those so in theory we shouldn't need to handle it as this stage
        subs = {s['subject_id']:s for s in subjects}

        dd = defaultdict(list)
        for s in samples:
            dd[s['sample_id']].append(s)
        samps = dict(dd)

        ### pools
        dd = defaultdict(list)
        for s, d in subs.items():
            if 'pool_id' in d:
                dd[d['pool_id']].append(s)
        sub_pools = dict(dd)
        pool_subs = {v:k for k, vs in sub_pools.items() for v in vs}

        dd = defaultdict(list)
        for s, d in samps.items():
            if d and 'pool_id' in d[0]:
                dd[d[0]['pool_id']].append(s)
        sam_pools = dict(dd)
        # XXX we can check for multiple pools per sample at various
        # stages including here, but technically a single specimen
        # being in multiple pools definitely happens
        pool_sams = {v:k for k, vs in sam_pools.items() for v in vs}

        records = []
        done_dirs = set()
        done_specs = set()

        ### performances
        union_perf = set(dirs) | set(perfs)
        inter_perf = set(dirs) & set(perfs)
        done_dirs.update(inter_perf)
        not_done_perfs = set(perfs) - inter_perf

        ### subject pools
        inter_sub_pool = set(dirs) & set(sub_pools)
        pooled_subjects = set(s for p, ss in sub_pools.items() if p in inter_sub_pool for s in ss)
        done_dirs.update(inter_sub_pool)

        ### subjects
        union_sub = set(dirs) | set(subs)
        inter_sub = set(dirs) & set(subs)

        if inter_sub | pooled_subjects == set(subs):
            ok_subs = subs
        else:
            ok_ids = inter_sub | pooled_subjects
            ok_subs = {k:v  for k, v  in subs.items() if k in ok_ids}
            # FIXME not all subjects have folders there may be samples
            # that have folders but not subjects ??? don't wan't to force
            # metadata structure onto folder structure but it complicates
            # the implementation again ... probably worth it in this case
            logd.warning('miscount subject dirs, TODO')

        for subject_id, blob in ok_subs.items():
            if subject_id in pool_subs:  # FIXME assumes 1:1 which is incorrect
                pool_id = pool_subs[subject_id]
            else:
                pool_id = None
            done_dirs.add(subject_id)
            done_specs.add(subject_id)
            records.append({'type': 'SubjectDirs',
                            # have to split the type because we can't recover
                            # the type using just the specimen id (sigh)
                            # and we need it to set the correct prefix (sigh)
                            'specimen_id': subject_id,
                            'dirs': [d[1] for d in dirs[subject_id]]
                            if subject_id in dirs else
                            [d[1] for d in dirs[pool_id]]})

        ### sample pools
        inter_sam_pool = set(dirs) & set(sam_pools)
        pooled_samples = set(s for p, ss in sam_pools.items() if p in inter_sam_pool for s in ss)
        done_dirs.update(inter_sam_pool)

        ### samples
        union_sam = set(dirs) | set(samps)
        inter_sam = set(dirs) & set(samps)

        template_version_less_than_2 = True  # FIXME TODO
        # FIXME this is where non-uniqueness of sample ids becomes a giant pita
        if inter_sam | pooled_samples == set(samps):
            for sample_id, blob in samps.items():
                if sample_id in pool_sams:  # FIXME assumes 1:1 which is incorrect
                    pool_id = pool_sams[sample_id]
                else:
                    pool_id = None

                if len(blob) > 1:
                    # FIXME TODO this means that we need to fail over to the primary keys
                    msg = f'sample_id is not unique! {sample_id}\n{blob}'
                    if he.addError(msg, blame='submission', path=path):
                        logd.error(msg)
                    continue

                if template_version_less_than_2:  # FIXME this is sure the cause an error at some point
                    done_dirs.add((blob[0]['subject_id'], sample_id))
                    done_specs.add(blob[0]['primary_key'])
                    id = blob[0]['primary_key']
                else:
                    done_dirs.add(sample_id)
                    done_specs.add(sample_id)
                    id = sample_id  # FIXME need ttl export suport for this

                records.append({'type': 'SampleDirs',
                                # have to split the type because we can't recover
                                # the type using just the specimen id (sigh)
                                # and we need it to set the correct prefix (sigh)
                                'specimen_id': id,
                                'dirs': [d[1] for d in dirs[sample_id]]
                                if sample_id in dirs else
                                [d[1] for d in dirs[pool_id]]})
        else:
            logd.warning('miscount sample dirs, TODO')
            bad_dirs = []
            if template_version_less_than_2:
                # handle old aweful nonsense
                # 1. construct subject sample lookups using tuple
                # 2. try to construct subject sample id pairs
                for sample_id, blobs in samps.items():
                    for blob in blobs:
                        if sample_id in dirs:
                            candidates = dirs[sample_id]
                            # TODO zero candidates error
                            actual = []
                            for level, drp, rparts in candidates:
                                if level < 2:
                                    msg = (f'Bad location for specimen folder! {drp}')
                                    if he.addError(msg,
                                                   blame='submission',
                                                   path=path):
                                        logd.error(msg)
                                    bad_dirs.append(dirs.pop(sample_id))
                                    continue
                                p_sample_id, p_subject_id, *p_rest = rparts
                                if level < 3:
                                    # p_subject_id will be primary derivatie or source
                                    log.warning(f'TODO new structure {drp}')

                                assert sample_id == p_sample_id  # this should always be true
                                subject_id = blob['subject_id']
                                if subject_id == p_subject_id:
                                    id = blob['primary_key']
                                    done_dirs.add((subject_id, p_sample_id))
                                    done_specs.add(id)
                                    actual.append(drp)

                            if actual:
                                records.append(
                                    {'type': 'SampleDirs',
                                    # have to split the type because we can't recover
                                    # the type using just the specimen id (sigh)
                                    # and we need it to set the correct prefix (sigh)
                                    'specimen_id': id,
                                    'dirs': actual,
                                    })
                        else:
                            msg = f'No folder for sample {sample_id}'
                            if he.addError(msg, blame='submission', path=path):
                                logd.error(msg)
            else:
                pass  # TODO that's an error!

        # handle nesting where parents may not have separate data
        # and this is why we want all ids to be unique per dataset
        # XXX at the moment performance metadata does not require a list of participating specimens
        # and we would have to infer it from the file system hierarchy which is not good
        # TODO populations and friends also an issue here
        _combined_index = {**perfs, **subs, **{v['primary_key']:v for vs in samps.values() for v in vs}}
        # XXX the real solution here is to add a column to the metadata sheets which
        # just makes it possible to assert that there this is a metadata only field
        # which I thought we had already discussed
        def getmaybe(blob):
            # TODO populations are a bit tricky here
            if 'sample_id' in blob:  # FIXME sigh missing types on these
                yield blob['subject_id']
                if 'was_derived_from' in blob:  # TODO member_of works backward with populations, so may need to deal with that case as well
                    parent = blob['was_derived_from']
                    if parent in _combined_index:
                        yield from getmaybe(_combined_index[parent])  # XXX watch out for bad circular refs here
                    else:
                        if parent.startswith('='):
                            msg = f'Sample was_derived_from specified as a formula. We do not currently support this.'
                        else:
                            msg = 'Sample was_derived_from does not exist!'

                        msg += f' {blob["sample_id"]!r} -> {parent!r}'

                        if he.addError(msg, blame='submission', path=path):
                            logd.error(msg)

            elif 'subject_id' in blob:
                return
            elif 'performance_id' in blob:
                return # XXX see note about non-required spec id above
            else:
                raise TypeError(f'unknown blob types {blob}')

        # for all done specs
        # check if there is a parent spec
        # and add it to the maybe done spec list
        maybe_done_specs_all = set(m for i in done_specs | inter_perf for m in getmaybe(_combined_index[i]) if m)
        maybe_not_done_specs = maybe_done_specs_all - set(done_specs)

        usamps = set(v['primary_key'] for vs in samps.values() for v in vs)
        udirs = set(nv for path_name, subpaths in dirs.items()
                    for nv in (((subpath[-1][1], path_name) for subpath in subpaths) # -1 rpaths 1 parent  # XXX FIXME clearly wrong ???
                               if path_name in samps else
                               (path_name,)))
        not_done_specs = (set(subs) | usamps) - set(done_specs)
        not_done_dirs = set(udirs) - set(done_dirs)

        def_not_done_specs = not_done_specs - maybe_not_done_specs

        obj = {}

        if records:
            obj['records'] = records
        else:
            pass # TODO embed an error

        if not_done_perfs:
            msg = ('There are performances that have no corresponding '
                   f'directory!\n{not_done_perfs}')
            if he.addError(msg,
                           blame='submission',
                           path=path):
                logd.error(msg)

        if maybe_not_done_specs:
            msg = ('Warning. Unmarked metadata-only specimens that have no corresponding '
                   f'directory, but have children that do.\n{maybe_not_done_specs}')
            if he.addError(msg,
                           blame='submission',
                           path=path):
                logd.warning(msg)

        if def_not_done_specs:
            msg = ('There are specimens that have no corresponding '
                   f'directory!\n{def_not_done_specs}')
            if he.addError(msg,
                           blame='submission',
                           path=path):
                logd.error(msg)

        if not_done_dirs:
            dists = {
                d: sorted(
                    [(nml.levenshteinDistance(d, s), s, source)
                     for ss, source in
                     (((def_not_done_specs | not_done_perfs), ' '),
                      ((done_specs | inter_perf), '*'),)
                     for s in ss])
                for d in not_done_dirs}
            align = {
                d: (lambda l: max(l) if l else -1)(
                    [len(s) for _, s, _ in dss])
                for d, dss in dists.items()}
            report = '\n\n'.join(
                [f'{" ".join([e[1].as_posix() for e in dirs[d]])}\n  0 {d}\n' + '\n'.join(
                    [f'{dist:>3} {s:<{align[d]}} {source}' for dist, s, source in
                     # unfortunately we can't limit the total number of entries because
                     [(dist, s, so) for dist, s, so in dists[d]
                      if dist < 10 or so == ' ' and dist < 15]])
                 for d in sorted(not_done_dirs)])

            msg = ('There are directories that have no corresponding '
                   f'metadata record!\n{not_done_dirs}')
            msg_report = msg + f'\nreport:\n{report}'
            if he.addError(msg + msg_report,
                           blame='submission',
                           path=path):
                logd.error(msg)

        he.embedErrors(obj)
        return obj,

    @staticmethod
    def study_description(study):
        contents = []
        for key in ('study_purpose', 'study_data_collection', 'study_primary_conclusion'):
            if key in study:
                contents.append(study[key])
        out = ' '.join(contents)
        return out,
