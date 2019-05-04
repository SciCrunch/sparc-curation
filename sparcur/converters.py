import rdflib
from types import GeneratorType
from sparcur.core import log, sparc
from pyontutils.core import OntId
from pyontutils.namespaces import TEMP, isAbout
from pyontutils.closed_namespaces import rdf, rdfs, owl
from scibot.extract import normalizeDoi
from pysercomb.pyr.units import ProtcParameter

a = rdf.type

class TripleConverter:
    # TODO consider putting mappings in a dict hierarchy
    # that reflects where they are in the schema??
    known_skipped = tuple()
    mapping = tuple()

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
        self._source = json_source
        self.integrator = integrator
        self.extra = self.Extra(self)

    def l(self, value):
        if isinstance(value, OntId):
            return value.u
        if isinstance(value, ProtcParameter):
            return value
        elif isinstance(value, str) and value.startswith('http'):
            return OntId(value).u
        else:
            return rdflib.Literal(value)

    def triples_gen(self, subject):
        if not isinstance(subject, rdflib.URIRef):
            subject = rdflib.URIRef(subject)

        for field, value in self._source.items():
            log.debug(f'{field}: {value}')
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
                    p, o = convert(v)
                    log.debug(o)
                    if isinstance(o, ProtcParameter):
                        s = rdflib.BNode()
                        text = o.for_triples
                        value, unit = text.split(' ')
                        yield subject, p, s
                        yield s, TEMP.hasValue, rdflib.Literal(value)
                        yield s, TEMP.hasUnit, rdflib.Literal(unit)

                    else:
                        yield subject, p, o

                    if extra is not None:
                        yield from extra(v)

            elif field in self.known_skipped:
                pass

            else:
                log.warning(f'Unhandled {self.__class__.__name__} field: {field}')


class ContributorConverter(TripleConverter):
    known_skipped = 'contributor_orcid_id', 'name'
    mapping = (
        ('first_name', sparc.firstName),
        ('last_name', sparc.lastName),
        ('contributor_affiliation', TEMP.hasAffiliation),
        ('is_contact_person', sparc.isContactPerson),
        ('is_responsible_pi', sparc.isContactPerson),
        )
 
    def contributor_role(self, value):
        return TEMP.hasRole, TEMP[value]

ContributorConverter.setup()


class MetaConverter(TripleConverter):
    mapping = [
        ['principal_investigator', TEMP.hasResponsiblePrincialInvestigator],
        ['protocol_url_or_doi', TEMP.hasProtocol],
        #['award_number', TEMP.hasAwardNumber],
        ['species', isAbout],
        ['organ', isAbout],
        ['modality', TEMP.hasExperimentalModality],
        ['subject_count', TEMP.hasNumberOfSubjects],
        ['uri_human', TEMP.hasHumanUri],
        ['keywords', isAbout],
    ]

    def protocol_url_or_doi(self, value):
        if 'doi' in value:
            doi = True
        elif value.startswith('10.'):
            value = 'doi:' + value
            doi = True
            
        if doi:
            value = OntId(normalizeDoi(value))
        else:
            value = rdflib.URIRef(value)

        return TEMP.hasProtocol, value

    def award_number(self, value): return TEMP.hasAwardNumber, TEMP[f'awards/{value}']
    class Extra:
        def __init__(self, converter):
            self.c = converter
            self.integrator = converter.integrator

        def award_number(self, value):
            _, s = self.c.award_number(value)
            yield s, a, owl.NamedIndividual
            yield s, a, TEMP.FundedResearchProject

        def protocol_url_or_doi(self, value):
            _, s = self.c.protocol_url_or_doi(value)
            yield s, a, owl.NamedIndividual
            yield s, a, sparc.Protocol
            pj = self.integrator.protocol(value)
            if pj:
                label = pj['protocol']['title']
                yield s, rdfs.label, rdflib.Literal(label)
                nsteps = len(pj['protocol']['steps'])
                yield s, TEMP.protocolHasNumberOfSteps, rdflib.Literal(nsteps)

            yield from self.integrator.triples_protcur(s)
MetaConverter.setup()  # box in so we don't forget


class DatasetConverter(TripleConverter):
    known_skipped = 'id', 'errors', 'inputs', 'subjects', 'meta', 'creators'
    mapping = [
        ['error_index', TEMP.errorIndex],
        ['submission_completeness_index', TEMP.submissionCompletenessIndex],
        ]
DatasetConverter.setup()


class SubjectConverter(TripleConverter):
    known_skipped = 'subject_id',
    mapping = [
        ['age_cateogry', TEMP.hasAgeCategory],
        ['species', sparc.animalSubjectIsOfSpecies],
        ['group', TEMP.hasAssignedGroup],
        #['rrid_for_strain', rdf.type],  # if only
        ['rrid_for_strain', sparc.specimenHasIdentifier],  # really subClassOf strain
        ['genus', sparc.animalSubjectIsOfGenus],
        ['species', sparc.animalSubjectIsOfSpecies],
        ['strain', sparc.animalSubjectIsOfStrain],
        ['weight', sparc.animalSubjectHasWeight],
        ['mass', sparc.animalSubjectHasWeight],
        ['sex', TEMP.hasBiologicalSex],
        ['gender', sparc.hasGender],
        ['age', TEMP.hasAge],
    ]
SubjectConverter.setup()
