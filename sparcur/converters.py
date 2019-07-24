from types import GeneratorType
import rdflib
from pyontutils import combinators as cmb
from pyontutils.namespaces import TEMP, TEMPRAW, isAbout
from pyontutils.closed_namespaces import rdf, rdfs, owl, dc
from scibot.extract import normalizeDoi
from pysercomb.pyr.units import Expr, _Quant as Quantity, Range
from sparcur import datasets as dat
from sparcur.core import OntId, OntTerm, lj, get_right_id
from sparcur.utils import log, logd, sparc
from sparcur.protocols import ProtocolData

a = rdf.type

class TripleConverter(dat.HasErrors):
    # TODO consider putting mappings in a dict hierarchy
    # that reflects where they are in the schema??
    known_skipped = tuple()
    mapping = tuple()

    _already_warned = set()

    class Extra:
        def __init__(self, converter):
            self.c = converter
            self.integrator = converter.integrator

    @classmethod
    def setup(cls):
        for attr, predicate in cls.mapping:
            def _func(self, value, p=predicate): return p, self.l(value)
            setattr(cls, attr, _func)

    def __init__(self, json_source, integrator=None):
        """ in case we want to do contextual things here """
        super().__init__()
        self._source = json_source
        self.integrator = integrator
        self.extra = self.Extra(self)

    def l(self, value):
        if isinstance(value, OntId):
            return value.u
        if isinstance(value, Expr):
            return value
        if isinstance(value, Quantity):
            return value
        elif isinstance(value, str) and value.startswith('http'):
            return OntId(value).u
        elif isinstance(value, dict):  # FIXME this is too late to convert?
            # NOPE! This idiot put a type field in his json dicts!
            if 'type' in value:
                if value['type'] == 'quantity':
                    return Quantity.fromJson(value)
                elif value['type'] == 'range':
                    return Range.fromJson(value)

            raise ValueError(value)
        else:
            return rdflib.Literal(value)

    def triples_gen(self, subject):
        if not isinstance(subject, rdflib.URIRef):
            subject = rdflib.URIRef(subject)

        for field, value in self._source.items():
            #log.debug(f'{field}: {value}')
            if type(field) is object:
                continue  # the magic helper key for Pipeline
            convert = getattr(self, field, None)
            extra = getattr(self.extra, field, None)
            if convert is not None:
                if isinstance(value, tuple) or isinstance(value, list):
                    values = value
                else:
                    values = value,

                for v in values:
                    log.debug(f'{field} {v} {convert}')
                    p, o = convert(v)
                    log.debug(o)
                    if isinstance(o, Expr) or isinstance(o, Quantity):
                        s = rdflib.BNode()
                        yield subject, p, s
                        qt = sparc.Measurement
                        if isinstance(o, Range):
                            yield from o.asRdf(s, quantity_rdftype=qt)
                        elif isinstance(o, Quantity):
                            yield from o.asRdf(s, rdftype=qt)
                        else:
                            log.warning(f'unhanded Expr type {o}')
                            yield from o.asRdf(s)
                    else:
                        yield subject, p, o

                    if extra is not None:
                        yield from extra(v)

            elif field in self.known_skipped:
                pass

            else:
                msg = f'Unhandled {self.__class__.__name__} field: {field}'
                if msg not in self._already_warned:
                    self._already_warned.add(msg)
                    log.warning(msg)
                    self.addError(msg, pipeline_stage=self.__class__.__name__ + '.export-error')


class ContributorConverter(TripleConverter):
    known_skipped = 'id', 'name'
    mapping = (
        ('first_name', sparc.firstName),
        ('middle_name', TEMP.middleName),
        ('last_name', sparc.lastName),
        ('contributor_affiliation', TEMP.hasAffiliation),
        ('affiliation', TEMP.hasAffiliation),  # FIXME iri vs literal
        ('is_contact_person', sparc.isContactPerson),
        ('is_responsible_pi', sparc.isContactPerson),
        ('blackfynn_user_id', TEMP.hasBlackfynnUserId),
        ('contributor_orcid_id', sparc.hasORCIDId),
        )
 
    def contributor_role(self, value):
        return TEMP.hasRole, TEMP[value]

    class Extra:
        def __init__(self, converter):
            self.c = converter
            self.integrator = converter.integrator

        def affiliation(self, value):
            #_, s = self.c.affiliation(value)
            yield from value.triples_gen
               

ContributorConverter.setup()


class MetaConverter(TripleConverter):
    mapping = [
        ['acknowledgements', TEMP.acknowledgements],
        ['folder_name', rdfs.label],
        ['title', dc.title],
        ['protocol_url_or_doi', TEMP.hasProtocol],
        #['award_number', TEMP.hasAwardNumber],
        ['species', isAbout],
        ['organ', isAbout],
        ['modality', TEMP.hasExperimentalModality],
        ['techniques', TEMP.protocolEmploysTechnique],
        ['uri_api', TEMP.hasUriApi],
        ['uri_human', TEMP.hasUriHuman],
        ['keywords', isAbout],
        ['description', dc.description],
        ['dirs', TEMP.hasNumberOfDirectories],
        ['files', TEMP.hasNumberOfFiles],
        ['size', TEMP.hasSizeInBytes],
        ['subject_count', TEMP.hasNumberOfSubjects],
        ['sample_count', TEMP.hasNumberOfSamples],
        ['contributor_count', TEMP.hasNumberOfContributors],

        ['title_for_complete_data_set', TEMP.collectionTitle],
        ['prior_batch_number', TEMP.Continues],  # see datacite relationType

        ['originating_article_doi', TEMP.IsDescribedBy],  # see relationType

        # TODO
        #['additional_links', ],
        #['completeness_of_data_set', ],
        #['examples'],
        #['links'],

    ]

    def principal_investigator(self, value):
        index = int(value.rsplit('/', 1)[-1])
        id = self.integrator.data['contributors'][index]['id']
        return TEMP.hasResponsiblePrincialInvestigator, rdflib.URIRef(id)  # FIXME reload -> ir

    def award_number(self, value): return TEMP.hasAwardNumber, TEMP[f'awards/{value}']
    class Extra:
        def __init__(self, converter):
            self.c = converter
            self.integrator = converter.integrator

        def award_number(self, value):
            _, s = self.c.award_number(value)
            yield s, a, owl.NamedIndividual
            yield s, a, TEMP.FundedResearchProject
            o = self.integrator.organ(value)
            if o:
                if o != 'othertargets':
                    o = OntId(o)
                    if o.prefix == 'FMA':
                        ot = OntTerm(o)
                        o = next(OntTerm.query(label=ot.label, prefix='UBERON')).OntTerm

                    yield s, isAbout, o.u

        def protocol_url_or_doi(self, value):
            _, s = self.c.protocol_url_or_doi(value)
            yield s, a, owl.NamedIndividual
            yield s, a, sparc.Protocol
            pd = ProtocolData(self.integrator.id)
            # FIXME needs to be a pipeline so that we can export errors
            pj = pd(value)  # FIXME a bit opaque, needs to move to a pipeline, clean up init etc.
            if pj:
                label = pj['protocol']['title']
                yield s, rdfs.label, rdflib.Literal(label)
                nsteps = len(pj['protocol']['steps'])
                yield s, TEMP.protocolHasNumberOfSteps, rdflib.Literal(nsteps)

            yield from self.integrator.triples_protcur(s)
MetaConverter.setup()  # box in so we don't forget


class DatasetConverter(TripleConverter):
    known_skipped = 'id', 'errors', 'inputs', 'subjects', 'meta', 'creators'
    mapping = []
DatasetConverter.setup()


class StatusConverter(TripleConverter):
    known_skipped = 'submission_errors', 'curation_errors'
    mapping = [
        ['submission_index', TEMP.submissionIndex],
        ['curation_index', TEMP.curationIndex],
        ['error_index', TEMP.errorIndex],
    ]
StatusConverter.setup()

file_related = [
    # file related
    # this is about the file about the sample, not the spamle itself
    # FIXME we don't want to export this, we want to use it to point
    # to a blackfynn package id, but that processing has to come during
    # the pipeline stage not here ...
    ['sha1', TEMP.hasDigitalArtifactThatIsAboutItWithHash],
    ['filename', TEMP.hasDigitalArtifactThatIsAboutIt],  #
    ['upload_filename', TEMP.hasDigitalArtifactThatIsAboutIt],
    ['dataset_id', TEMP.providerDatasetIdentifier],  # FIXME need a global convention for this
    ]
execution_related = [

    ['experiment_number', TEMP.localExecutionNumber],  # FIXME TODO
    ['session', TEMP.localExecutionNumber],
]

utility = [
    ['comment', TEMP.providerNote],
    ['note', TEMP.providerNote],
]
class SubjectConverter(TripleConverter):
    mapping = [
        ['subject_id', TEMP.localId],
        ['ear_tag_number', TEMP.localIdAlt],
        ['age_category', TEMP.hasAgeCategory],
        ['group', TEMP.hasAssignedGroup],
        ['treatment', TEMP.TODO],
        #['rrid_for_strain', rdf.type],  # if only
        ['rrid_for_strain', sparc.specimenHasIdentifier],  # really subClassOf strain
        ['genus', sparc.animalSubjectIsOfGenus],
        ['species', sparc.animalSubjectIsOfSpecies],
        ['strain', sparc.animalSubjectIsOfStrain],
        ['genotype', TEMP.hasGenotype],
        ['weight', sparc.animalSubjectHasWeight],
        ['initial_weight', sparc.animalSubjectHasWeight],  # TODO time
        ['mass', sparc.animalSubjectHasWeight],
        ['body_mass', sparc.animalSubjectHasWeight],  # TODO
        ['height_inches', TEMP.subjectHasHeight],  # FIXME converted in datasets sigh
        ['sex', TEMP.hasBiologicalSex],
        ['gender', sparc.hasGender],
        ['age', TEMP.hasAge],
        ['age_years', TEMP.hasAge],  # FIXME this is ok because we catch it when converting but
        # it is yet another case where things are decoupled

        ['body_mass_weight', TEMP.subjectHasWeight],  # FIXME human vs animal, sigh

        ['anesthesia', TEMP.wasAdministeredAnesthesia],
        # TODO experiment hasParticpant (some (subject wasAdministeredAnesthesia))
        # or something like that -> experimentHasSubjectUnder ...


        ['stimulation_site', sparc.spatialLocationOfModulator],  # TODO ontology
        ['stimulator', sparc.stimulatorUtilized],
        ['stimulating_electrode_type', sparc.stimulatorUtilized],  # FIXME instance vs type

        ['organism_rrid', TEMP.TODO],

        ['nerve', TEMPRAW.involvesAnatomicalRegion],  # FIXME we can be more specific than this probably

        # require extras
        ['experiment_date', TEMP.protocolExecutionDate],  # FIXME needs to reference a protocol
        ['injection_date', TEMP.protocolExecutionDate],  # FIXME needs to reference a protocol
        ['date_of_euthanasia', TEMP.protocolExecutionDate],
        ['date_of_injection', TEMP.protocolExecutionDate],
    ] + file_related + execution_related + utility
SubjectConverter.setup()


class SampleConverter(TripleConverter):
    mapping = [
        ['sample_id', TEMP.localId],  # unmangled
        #['subject_id', TEMP.wasDerivedFromSubject],
        ['specimen_anatomical_location', TEMPRAW.wasExtractedFromAnatomicalRegion],
        ['sample_name', rdfs.label],
        ['sample_description', dc.description],
        ['group', TEMP.hasAssignedGroup],

        ['specimen', TEMP.TODO],  # sometimes used as anatomical location ...
        ['tissue', TEMP.TODO],
        ['counterstain', TEMP.TODO],
        ['muscle_layer', TEMP.TODO],

        # complex conversions required here ...
        ['target_electrode_location_relative_to_greater_lesser_curvature', TEMP.TODO],  # YEEEE aspects!
        ['target_electrode_orientation_relative_to_local_greater_curvature', TEMP.TODO],  # :D
        ['electrode_implant_location', TEMP.TODO],
        ['stimulation_electrode___chronic_or_acute', TEMP.TODO],
        ['includes_chronically_implanted_electrode_', TEMP.TODO],
    ] + file_related + execution_related + utility
    def subject_id(self, value):
        # FIXME _subject_id is monkey patched in
        return TEMP.wasDerivedFromSubject, self._subject_id(value)
SampleConverter.setup()

class ApiNATOMYConverter(TripleConverter):
    @staticmethod
    def apinatbase():
        # TODO move it external file
        yield TEMP.isAdvectivelyConnectedTo, a, owl.ObjectProperty
        yield TEMP.isAdvectivelyConnectedTo, a, owl.SymmetricProperty
        yield TEMP.isAdvectivelyConnectedTo, a, owl.TransitiveProperty

        yield TEMP.advectivelyConnects, a, owl.ObjectProperty
        yield TEMP.advectivelyConnects, a, owl.TransitiveProperty

        yield TEMP.advectivelyConnectsFrom, a, owl.ObjectProperty
        yield TEMP.advectivelyConnectsFrom, rdfs.subPropertyOf, TEMP.advectivelyConnects

        yield TEMP.advectivelyConnectsTo, a, owl.ObjectProperty
        yield TEMP.advectivelyConnectsTo, rdfs.subPropertyOf, TEMP.advectivelyConnects
        yield TEMP.advectivelyConnectsTo, owl.inverseOf, TEMP.advectivelyConnectsFrom

        idct = TEMP.isDiffusivelyConnectedTo
        yield idct, a, owl.ObjectProperty
        yield idct, a, owl.SymmetricProperty
        yield idct, a, owl.TransitiveProperty  # technically correct modulate the concentration gradient

        yield TEMP.diffusivelyConnects, a, owl.ObjectProperty
        yield TEMP.diffusivelyConnects, a, owl.TransitiveProperty

        yield TEMP.diffusivelyConnectsFrom, a, owl.ObjectProperty
        yield TEMP.diffusivelyConnectsFrom, rdfs.subPropertyOf, TEMP.advectivelyConnects

        yield TEMP.diffusivelyConnectsTo, a, owl.ObjectProperty
        yield TEMP.diffusivelyConnectsTo, rdfs.subPropertyOf, TEMP.advectivelyConnects
        yield TEMP.diffusivelyConnectsTo, owl.inverseOf, TEMP.advectivelyConnectsFrom

    def materialTriples(self, subject, link, predicate):
        rm = self._source

        def yield_from_id(s, matid, predicate=predicate):
            mat = rm[matid]
            if 'external' in mat:
                mat_s = OntTerm(mat['external'][0])
                yield s, predicate, mat_s.u
                yield mat_s.u, a, owl.Class
                yield mat_s.u, rdfs.label, rdflib.Literal(mat_s.label)
                if 'materials' in mat:
                    for submat_id in mat['materials']:
                        yield from yield_from_id(mat_s, submat_id, TEMP.hasConstituent)

            else:
                log.warning(f'no external id for {mat}')

        for matid in link['conveyingMaterials']:
            yield from yield_from_id(subject, matid)

    @property
    def triples_gen(self):
        rm = self._source

        # FIXME there doesn't seem to be a section that tells me the name
        # of top level model so I have to know its name beforhand
        # the id is in the model, having the id in the resource map
        # prevents issues if these things get sent decoupled
        id = rm['id']
        mid = id.replace(' ', '-')

        links = rm[id]['links']
        #linknodes = [n for n in rm[id]['nodes'] if n['class'] == 'Link']  # visible confusion

        st = []
        from_to = []
        ot = None
        yield from self.apinatbase()
        for link in links:
            if 'conveyingType' in link:
                if link['conveyingType'] == 'ADVECTIVE':
                    p_is =   TEMP.isAdvectivelyConnectedTo
                    p_from = TEMP.advectivelyConnectsFrom
                    p_to =   TEMP.advectivelyConnectsTo
                    p_cmat = TEMP.advectivelyConnectsMaterial
                    diffusive = False
                elif link['conveyingType'] == 'DIFFUSIVE':
                    p_is =   TEMP.isDiffusivelyConnectedTo
                    p_from = TEMP.diffusivelyConnectsFrom
                    p_to =   TEMP.diffusivelyConnectsTo
                    p_cmat = TEMP.diffusivelyConnectsMaterial
                    diffusive = True
                else:
                    log.critical(f'unhandled conveying type {link}')
                    continue

                source = link['source']
                target = link['target']
                ok = True
                if len(from_to) == 2:  # otherwise
                    st = []
                    from_to = []
                for i, e in enumerate((source, target)):
                    ed = rm[e]
                    if 'external' not in ed:
                        if not i and from_to:
                            # TODO make sure the intermediate ids match
                            pass
                        else:
                            ok = False
                            break
                    else:
                        st.append(e)
                        from_to.append(OntId(ed['external'][0]))

                conveying = link['conveyingLyph']
                cd = rm[conveying]
                if 'external' in cd:
                    old_ot = ot
                    ot = OntTerm(cd['external'][0])
                    yield ot.u, a, owl.Class
                    yield ot.u, TEMP.internalId, rdflib.Literal(conveying)
                    yield ot.u, rdfs.label, rdflib.Literal(ot.label)

                    yield from self.materialTriples(ot.u, link, p_cmat)  # FIXME locate this correctly

                    if ok:
                        u, d = from_to
                        if st[0] == source:
                            yield u, rdfs.label, rdflib.Literal(OntTerm(u).label)
                            yield u, a, owl.Class
                            yield from cmb.restriction.serialize(ot.u, p_from, u)

                        if st[1] == target:
                            yield d, rdfs.label, rdflib.Literal(OntTerm(d).label)
                            yield d, a, owl.Class
                            yield from cmb.restriction.serialize(ot.u, p_to, d)

                    if old_ot is not None and old_ot != ot:
                        yield from cmb.restriction.serialize(ot.u, p_from, old_ot.u)

                if diffusive:
                    # we can try to hack this using named individuals
                    # but it is not going to do exactly what is desired
                    s_link = TEMP[f'ApiNATOMY/{mid}/{link["id"]}']
                    s_cd = TEMP[f'ApiNATOMY/{mid}/{cd["id"]}']
                    yield s_link, a, owl.NamedIndividual
                    yield s_link, a, TEMP.diffusiveLink  # FIXME I'm not sure these go in the model ...
                    yield s_cd, a, owl.NamedIndividual
                    if 'external' in cd and cd['external']:
                        oid = OntId(cd['external'][0])
                        yield s_cd, a, oid.u
                        ot = oid.asTerm
                        if ot.label:
                            yield oid.u, rdfs.label, ot.label

                    else:
                        yield s_cd, a, TEMP.conveyingLyph
                        for icd in cd['inCoalescences']:
                            dcd = rm[icd]
                            log.info(lj(dcd))
                            s_icd = TEMP[f'ApiNATOMY/{mid}/{dcd["id"]}']
                            yield s_cd, TEMP.partOfCoalescence, s_icd
                            yield s_icd, a, owl.NamedIndividual
                            yield s_icd, a, TEMP['ApiNATOMY/Coalescence']
                            if 'external' in dcd and dcd['external']:
                                oid = OntId(dcd['external'][0])
                                yield s_icd, a, oid.u
                                ot = oid.asTerm
                                if ot.label:
                                    yield oid.u, rdfs.label, ot.label

                            for lyphid in dcd['lyphs']:
                                ild = rm[lyphid]
                                log.info(lj(ild))
                                if 'external' in ild and ild['external']:
                                    yield s_icd, TEMP.hasLyphWithMaterial, OntId(ild['external'][0])

                if not ok:
                    logd.info(f'{source} {target} issue')
                    continue

                for inid, e in zip(st, from_to):
                    yield e.u, a, owl.Class
                    yield e.u, rdfs.label, rdflib.Literal(OntTerm(e).label)
                    yield e.u, TEMP.internalId, rdflib.Literal(inid)

                f, t = from_to
                yield from cmb.restriction.serialize(f.u, p_is, t.u)

ApiNATOMYConverter.setup()
