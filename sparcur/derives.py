import copy
from typing import Tuple
from functools import wraps
import idlib
from sparcur import schemas as sc
from sparcur import normalization as nml
from sparcur.core import log, logd, JPointer, HasErrors


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
        if ',' in name:
            last, first = name.split(', ', 1)
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
        doi = idlib.Doi(doi_string)
        try:
            metadata = doi.metadata()  # FIXME network sandbox violation
            if metadata is not None:
                return doi
        except idlib.exceptions.ResolutionError:
            # sometimes a doi is present on the platform but does not resolve
            # in which case we don't add it as metadata because it has not
            # been officially published, just reserved, this check is more
            # correct than checkin the status on the platform
            # FIXME HOWEVER it violates the network sandbox, so we probably
            # need an extra step during the data retrieval phase which attempts
            # to fetch all the doi metadata
            pass

    @staticmethod
    def _lift_mr(path_dataset, dataset_relative_path, record, should_log):
        parent = dataset_relative_path.parent
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

        lifted['prov'] = dataset_relative_path
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
        if 'mimetype' in lifted and lifted['mimetype'] == 'inode/vnd.abi.scaffold+directory':
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
            if 'contents' in manifest:
                contents = manifest['contents']
                if 'manifest_records' in contents:
                    drp = manifest['dataset_relative_path']
                    _should_log = True
                    for record in contents['manifest_records']:
                        lifted, _should_log = cls._lift_mr(
                            path_dataset, drp, record, _should_log)
                        path_metadata.append(lifted)
                        if 'errors' not in lifted:
                            cls._scaffolds(lifted, scaffolds)

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
