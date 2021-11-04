import idlib
import ontquery as oq
import augpathlib as aug
from github import Github
from pyontutils import obo_io as oio
from pyontutils import sheets
from pyontutils.core import OntResIri, OntId
from pyontutils.config import auth
from pyontutils.namespaces import ilxtr

# for reference see
# https://github.com/obophenotype/uberon/blob/master/uberon-idranges.owl
# Datatype: idrange:22
# Annotations: allocatedto: "InterLex",
# EquivalentTo: xsd:integer[> 8310000, <= 8399999]
# hard coding due to lack of a manchester syntax parser
uberon_id_range = 8310000, 8400000


def github_url_to_group_repo(url_string):
    # FIXME github_url_to_python_object is what we really want
    # but that requires a bit more setup via orthauth
    from urllib.parse import urlparse
    url_obj = urlparse(url_string)
    netloc = url_obj.netloc
    if netloc in ('github.com',  'raw.githubusercontent.com'):
        group_repo = '/'.join(url_obj.path.split('/', 3)[1:3])
        return group_repo
    else:
        raise ValueError(f"Don't know how to handle uris for {netloc}")


class Hrm(OntResIri):

    def submit_terms_to(self, upstream_iri, *identifiers, **kwargs):
        pass


class OntTerm(idlib.from_oq.OntTerm):

    def asOboTerm(self, *args, id='tgbugsTODO'):
        graph = self.query.services[0].graph
        ilx_id = OntId(self).u
        s = list(graph[:ilxtr.hasIlxId:ilx_id])[0]
        if s != ilx_id:
            self = self.__class__(s)
            self.fetch()

        try:
            term = oio.Term(id=id, name=self.label)
            if not self.definition:
                raise ValueError(f'{self} is misisng a definition!')

            defxrefs = list(graph[s:OntId('ilx.anno.hasDefinitionSource:').u:])
            term.add(oio.TVPair(tag='def', text=self.definition, xrefs=defxrefs))

            for synonym in (self.synonyms
                            if isinstance(self.synonyms, tuple) else  # FIXME ontquery issue
                            (self.synonyms,)):
                term.add(oio.TVPair(tag='synonym', text=synonym, typedef='EXACT'))

            # FIXME SIGH brokenness of OntTerm.__call__ return types
            ps = ('ilx.anno.hasBroadSynonym:', 'oboInOwl:hasBroadSynonym')
            broads = [v for k, vs in self(*ps).items() if k in ps for v in vs]
            for bs in broads:
                term.add(oio.TVPair(tag='synonym', text=bs, typedef='BROAD'))

            ps = ('ilx.anno.hasRelatedSynonym:', 'oboInOwl:hasRelatedSynonym')
            relateds = [v for k, vs in self(*ps).items() if k in ps for v in vs]
            for rs in relateds:
                term.add(oio.TVPair(tag='synonym', text=rs, typedef='RELATED'))

            for iri in graph[s:ilxtr.hasExistingId]:
                term.add(oio.TVPair(tag='xref', name=OntId(iri).curie,))

        except (AttributeError, TypeError) as e:
            raise ValueError(self) from e

        return term


class Row(sheets.Row):

    _repo_objs = {}
    _pr_objs = {}

    @classmethod
    def github_api(cls):
        if not hasattr(cls, '_github_api'):
            cls._github_api = Github(auth.user_config.secrets(
                'github',
                'tgbugs-build',  # FIXME obvs
                'WARNING-development-all-scopes'))

        return cls._github_api

    @classmethod
    def pr_repo(cls, group_repo):
        if group_repo not in cls._repo_objs:
            gh_repo_obj = cls.github_api().get_repo(group_repo)
            cls._repo_objs[group_repo] = gh_repo_obj
        return cls._repo_objs[group_repo]

    @property
    def id(self):
        return OntTerm(self.interlex_id().value)

    @property
    def ready(self):
        return self.ready_to_go().value

    @property
    def submitted(self):
        test = 'https://github.com/obophenotype/uberon/pull/'
        return self.pull_request_link().value.startswith(test)
        #return self.status().value == 'pr submitted'

    @property
    def merged(self):
        prv = self.pull_request_link()
        url = prv.value
        group_repo = github_url_to_group_repo(url)
        repo = self.pr_repo(group_repo)
        _, pr_number_string = url.rsplit('/', 1)
        pr_number = int(pr_number_string)
        # lol github using the same index for prs an issues useful to
        # simplify referencing things, but means that you don't know
        # the type without checking it when you only have the number
        if pr_number not in self._pr_objs:
            self._pr_objs[pr_number] = repo.get_pull(pr_number)

        pr = self._pr_objs[pr_number]
        return pr.merged

    @property
    def to_submit(self):
        return self.ready and not self.merged  # submitted terms may need to be updated


# monkey patch
sheets.Row = Row


class UpstreamTermRequests(sheets.Sheet):
    name = 'sparc-upstream-terms'
    sheet_name = 'upstream'
    index_columns = 'interlex_id',

    def _row_objects(self):
        ros = [self.row_object(i) for i, v  in enumerate(self.values)][1:]
        return ros

    def ready(self):
        return [r for r in self._row_objects() if r.ready]
    
    def submitted(self):  # FIXME should be on row
        return [r for r in self._row_objects() if r.submitted]

    def merged(self):  # FIXME should be on row
        return [r for r in self._row_objects() if r.merged]

    def to_submit(self):
        # TODO actually check uberon
        return [r for r in self._row_objects() if r.to_submit]

    def submit_to_obofile(self, of, prefix, id_range):
        terms = [ro.id for ro in self.to_submit()]
        [t.fetch() for t in terms]
        #obo_terms = [t.asOboTerm(id=f'tgbugsTODO{i}') for i, t in enumerate(terms)]
        id_min, id_max = id_range
        over_under = [
            int(b.id_.value.suffix)
            for b in of.Terms.values()
            if not isinstance(b, list) and ':' in b.id_.value
            and id_min < int(OntId(b.id_.value).suffix) < id_max]
        if over_under:
            id_start = max(over_under) + 1
        else:
            id_start = id_min

        # TODO padding and prefix detect etc.
        obo_terms = [t.asOboTerm(id=f'{prefix}:{id_start + i}') for i, t in enumerate(terms)]
        of.add(*obo_terms)


def main():
    #ori = OntResIri('https://alt.olympiangods.org/sparc/ontologies/community-terms.ttl')
    ori = OntResIri('http://localhost:8515/sparc/ontologies/community-terms.ttl')
    rdfl = oq.plugin.get('rdflib')(ori.graph, OntId)
    OntTerm.query_init(rdfl)
    utr = UpstreamTermRequests()
    _todo(utr)
    return

    # test output
    tof = oio.OboFile()
    utr.submit_to_obofile(tof, 'FIXME', (0, float('inf')))
    tof.write(path=aug.RepoPath('/tmp/uberon-new-terms.obo'), overwrite=True)

    ori_uberon = OntResIri('http://purl.obolibrary.org/obo/uberon.owl')
    ori_uberon_meta = ori_uberon.metadata()  # doap:GitRepository
    breakpoint()
    return


def _todo(utr):
    # real output
    glb = auth.get_path('git-local-base')
    uberon_edit = aug.RepoPath(glb) / 'NOFORK/uberon/src/ontology/uberon-edit.obo'
    of = oio.OboFile(path=uberon_edit, strict=False)
    utr.submit_to_obofile(of, 'UBERON', uberon_id_range)
    of.write(overwrite=True,
             version=oio.OBO_VER_ROBOT)

    # TODO 
    # source-iri + identifiers -> target-iri
    # need a target-iri -> edit type + edit id
    # e.g for uberon if there is a branch where we have stuck our working pull requests
    # that goes in the edit id.
    # https://alt.olympiangods.org/sparc/ontologies/community-terms.ttl + ids ->
    # http://purl.obolibrary.org/obo/uberon.owl -> 
    # https://github.com/obophenotype/uberon/tree/sparc-term-request-flow/blob/uberon_edit.obo


if __name__ == '__main__':
    main()
