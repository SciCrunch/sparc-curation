import copy
from typing import Tuple
from functools import wraps
from collections import defaultdict
import idlib
from sparcur import schemas as sc
from sparcur import exceptions as exc
from sparcur import normalization as nml
from sparcur.core import log, logd, JPointer, HasErrors, hashable_converter
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
    def ident_multi(id_string):
        """ split whitespace separated lists of identifiers """
        ids = tuple(id_string.split())
        raise NotImplementedError('this should be done in the normalization pass from tabular not here')
        return ids

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
    def dataset_species(collected=tuple(), model=tuple()) -> Tuple[tuple]:
        out = set(collected) | set(model)
        if len(out) == 1:  # FIXME legacy support just make this a list
            return next(iter(out))

        return tuple(out)

    @staticmethod
    def dataset_subject_species(subjects) -> Tuple[tuple]:
        out = set()
        for subject in subjects:
            if 'species' in subject:
                # FIXME virtual species ... yeah bad design keep users happy oops
                # XXX probably this means we should filter virtual subjects from
                # subjects and keep them in a separate list
                out.add(subject['species'])

        return tuple(out)

    @staticmethod
    def dataset_manifest_organ(manifest_file) -> Tuple[tuple]:
        return Derives.dataset_manifest_thing(manifest_file, field='organ')

    @staticmethod
    def dataset_manifest_species(manifest_file) -> Tuple[tuple]:
        return Derives.dataset_manifest_thing(manifest_file, field='species')

    @staticmethod
    def dataset_manifest_thing(manifest_file, field) -> Tuple[tuple]:
        out = set()
        for mf in manifest_file:
            if 'contents' in mf:
                contents = mf['contents']
                if 'manifest_records' in contents:
                    for record in contents['manifest_records']:
                        if field in record:
                            # FIXME break if we don't find it so we don't search every row probably
                            # FIXME do we drop empty keys for manifests?
                            # FIXME should we keep the header as a schema?
                            out.add(record[field])

                out.add

        return tuple(out)

    @staticmethod
    def doi(doi_string):  # FIXME massive network sandbox violation here
        """ check if a doi string resolves, if it does, return it """
        if doi_string is None:
            raise TypeError('WHAT HAVE YOU DONE!?')

        doi = idlib.Doi(doi_string)
        return doi
        # XXX old network check below, causes numerous issues when datasets
        # are published because their latest metadata has no doi
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
        _should_log = should_log
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
                    _should_log = False
                    logd.error(message)

            he.embedErrors(lifted)

        # have to reattach path_dataset because Path.cwd() may not be
        # the dataset root (usually the organizaiton root)
        _repath = path_dataset / dataset_relative_path
        try:
            lifted['prov:wasDerivedFrom'] = _repath.cache_identifier
        except exc.NoStreamError as e:
            # cache not present e.g. due to local modification so no
            # remote id is available, so we leave the prov record out,
            # though we could use a checksum instead
            msg = f'missing cache metadata: {_repath}'
            if should_log:
                _should_log = False
                logd.warning(msg)

        lifted['dataset_relative_path'] = record_drp
        lifted['manifest_record'] = {
            k:v for k, v in record.items() if k != 'filetype'
        }
        if 'additional_types' in record:
            # FIXME TODO make sure that the mimetypes match
            # FIXME currently a string, either change name or make a list?
            lifted['mimetype'] = record['additional_types']

        return lifted, _should_log

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
    def samples_rekey(samples):
        """ use to rekey primary_key to be sample_id for sds version > 2 """
        for sample in samples:
            sample['primary_key'] = sample['sample_id']

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
        _subjects = [{k:v for k, v in blob_sample.items() if k in subject_fields}
                     for blob_sample in samples]
        subjects = [dict(s) for s in set(hashable_converter(d) for d in _subjects)]
        return subjects,

    @staticmethod
    def validate_structure(path, dir_structure, path_metadata, # manifests,
                           performances, subjects, samples, sites):
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

        ent_by_manifest = {}
        for pmr in path_metadata:
            if 'manifest_record' in pmr:
                pmrmr = pmr['manifest_record']
                for field in('entity', 'specimen', 'subject', 'sample', 'site', 'performance',):
                    if field in pmrmr:
                        fv = pmrmr[field]
                        for ent_id in fv:
                            ent_by_manifest[ent_id] = (
                                pmr['dataset_relative_path'],
                                (pmr['remote_id'] if 'remote_id' in pmr else None),)

        ent_done_by_manifest = set(ent_by_manifest)
        # FIXME TODO the next step is accounting for every single
        # _file_ not just every folder, this means we have to handle
        # the non-transitive case so we don't count parent folders
        # unless they are explicit i think?  this gets quite a bit
        # more complex here :/

        perfs = {p['performance_id']:p for p in performances}
        metadata_only_perfs = set(k for k, v in perfs.items() if 'metadata_only' in v and v['metadata_only'])

        site_records = sites
        sites = {p['site_id']:p for p in site_records}
        metadata_only_sites = set(k for k, v in sites.items() if 'metadata_only' in v and v['metadata_only'])

        # subject_id could be missing, but we filter failures on all of
        # those so in theory we shouldn't need to handle it as this stage
        subs = {s['subject_id']:s for s in subjects}

        dd = defaultdict(list)
        for s in samples:
            dd[s['sample_id']].append(s)
        samps = dict(dd)

        ### pools
        metadata_only_specs = set()  # TODO also need metadata_only sites and perfs etc.
        dd = defaultdict(list)
        for s, d in subs.items():
            if 'metadata_only' in d and d['metadata_only']:
                metadata_only_specs.add(s)

            if 'pool_id' in d:
                dd[d['pool_id']].append(s)
        sub_pools = dict(dd)
        pool_subs = {v:k for k, vs in sub_pools.items() for v in vs}

        dd = defaultdict(list)
        for s, d in samps.items():
            if d and 'metadata_only' in d[0] and d[0]['metadata_only']:
                metadata_only_specs.add(s)

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
        not_done_perfs = set(perfs) - (inter_perf | metadata_only_perfs | ent_done_by_manifest)

        ### sites
        union_site = set(dirs) | set(sites)
        inter_site = set(dirs) & set(sites)
        done_dirs.update(inter_site)
        not_done_sites = set(sites) - (inter_site | metadata_only_sites | ent_done_by_manifest)

        ### subject pools
        inter_sub_pool = set(dirs) & set(sub_pools)
        pooled_subjects = set(s for p, ss in sub_pools.items() if p in inter_sub_pool for s in ss)
        done_dirs.update(inter_sub_pool)

        ### subjects
        union_sub = set(dirs) | set(subs)
        inter_sub = set(dirs) & set(subs)

        #if inter_sub | pooled_subjects == (set(subs) - (metadata_only_specs | ent_done_by_manifest)):
        if inter_sub | pooled_subjects == (set(subs) - metadata_only_specs):  # FIXME exact equality here causing issues
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
        #if inter_sam | pooled_samples == (set(samps) - (metadata_only_specs | ent_done_by_manifest)):
        if inter_sam | pooled_samples == (set(samps) - metadata_only_specs):  # FIXME exact equality here causing issues
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

                if sample_id in metadata_only_specs:
                    if sample_id in inter_sam:
                        # TODO embed this one probably?
                        breakpoint()
                        logd.error(f'metadata only sample has a folder??? {sample_id}')

                    continue

                if template_version_less_than_2:  # FIXME this is sure the cause an error at some point
                    if '_' in blob[0]['primary_key']:  # composite primary case
                        done_dirs.add((blob[0]['subject_id'], sample_id))
                    else:
                        done_dirs.add(sample_id)

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
                                subject_id = blob['subject_id'] if 'subject_id' in blob else None
                                if subject_id == p_subject_id:
                                    id = blob['primary_key']
                                    if '_' in id:  # composite primary key
                                        done_dirs.add((subject_id, p_sample_id))
                                    else:
                                        done_dirs.add(p_sample_id)

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
                            if sample_id in ent_done_by_manifest:
                                logd.info(f'done by manifest {sample_id}')
                                continue

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
                    parents = blob['was_derived_from']
                    for parent in parents:
                        parent_pkey = blob['subject_id'] + '_' + parent
                        if parent_pkey in _combined_index:
                            # this branch should pretty much never trigger now
                            yield from getmaybe(_combined_index[parent_pkey])  # XXX watch out for bad circular refs here
                        elif parent in _combined_index:
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
        _composite_primary = set(s for s in usamps if '_' in s)
        # udirs is a set of strings and tuples, strings for top level
        # folders that are not nested and tuples for samples folders,
        # matching how done_dirs is populated above
        _udirs = set(
            nv for path_name, matched_subpaths in dirs.items() for nv in
            (((subject, path_name) for subject in
              set(element for depth, path, elements in matched_subpaths
                  for element in elements if element.startswith('sub-')))
             if path_name in samps and _composite_primary else
             (path_name,)))
        udirs = _udirs  # if _composite_primary else set(dirs)
        not_done_specs = (set(subs) | usamps) - (set(done_specs) | metadata_only_specs | ent_done_by_manifest)
        not_done_dirs = set(udirs) - set(done_dirs)

        spd = {d: set(p[-1][-1] for p in dirs[d]) for d in done_dirs}
        # TODO reconcile with manifest mapping as well
        not_in_primary = set(k for k, v in spd.items() if 'primary' not in v)

        def_not_done_specs = not_done_specs - maybe_not_done_specs

        double_done_ents = done_dirs & ent_done_by_manifest
        # TODO in a nested setting there are cases where a path mapped to an entity might contain
        # multiple parent structures so we want to detect those cases because things get a bit tricky
        # with multiple nesting and I don't have a complete understanding of all the cases yet
        ent_possibly_mismatched = set((k, p) for k, (a, b) in ent_by_manifest.items() for p in a.parts if p in done_dirs)
        # we want to catch cases where there are any done parent paths that are not exactly mapped to the entity
        # because we need to use them for comparison against the hierarchy from the metadata sheets, nesting in
        # folders that does not match the metadata is critical to catch

        # we also want to detect cases where a dir sub-1 has a manifest record e.g. sam-1
        # not just a case where a file sub-1/file-1.ext has a manifest record sam-1 and for that
        # we need the more restrictive case checking just the name of the manifest record
        ent_dir_mismatch = set((k, a.name) for k, (a, b) in ent_by_manifest.items() if a.name in done_dirs and a.name != k)

        not_actually_metadata_only = (metadata_only_specs | metadata_only_sites | metadata_only_perfs) & (done_dirs | ent_done_by_manifest)

        obj = {}

        if records:
            obj['records'] = records
        else:
            pass # TODO embed an error

        # FIXME TODO dirs without files case

        if double_done_ents:
            # this is not necessarily a problem but is good to catch
            msg = ('There are entities with double mapping via directory name and manifest!'
                   f'\n{double_done_ents}')
            if he.addError(msg,
                           blame='submission',
                           path=path):
                logd.error(msg)

        if ent_dir_mismatch:
            # catch cases where a directory name and the mapping are mismatched
            msg = ('There are directories with mismatched manifest record entity mapping!'
                   f'\n{ent_dir_mismatch}')
            if he.addError(msg,
                           blame='submission',
                           path=path):
                logd.error(msg)

        if not_actually_metadata_only:
            # catch cases where ents marked as metadata only actually have a dir or manifest record
            # TODO better error message for these, specifically the full path or the metadata entry
            msg = ('There are entities marked as metadata only that have a directory or manifest entry!'
                   f'\n{not_actually_metadata_only}')
            if he.addError(msg,
                           blame='submission',
                           path=path):
                logd.error(msg)

        if not_done_perfs:
            msg = ('There are performances that have no corresponding '
                   f'directory!\n{not_done_perfs}')
            if he.addError(msg,
                           blame='submission',
                           path=path):
                logd.error(msg)

        if not_done_sites:
            msg = ('There are sites that have no corresponding '
                   f'directory!\n{not_done_sites}')
            if he.addError(msg,
                           blame='submission',
                           path=path):
                logd.error(msg)

        if maybe_not_done_specs:
            # FIXME producing incorrect error here when there is a folder but NOT metadata
            # this is strange, this is happening because there is a reference to a subject
            # in a samples sheet that is not in the subjects sheet, that is a separate error
            # FIXME this also means that there is some asymmetry between matching folders
            # and matching specimens because the samples only sheet accounts for the
            # folder but the folder does not account for subjects appearing only in the
            # samples sheet subject_id column
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
                for d in (sot[-1] if isinstance(sot, tuple) else sot
                          for sot in not_done_dirs)
            }
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
                 for d in sorted(sot[-1] if isinstance(sot, tuple) else sot
                                 for sot in not_done_dirs)])
            msg = ('There are directories that have no corresponding '
                   f'metadata record!\n{not_done_dirs}')
            msg_report = msg + f'\nreport:\n{report}'
            if he.addError(msg_report,
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
