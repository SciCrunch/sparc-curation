import json
import itertools
from collections import defaultdict
from urllib.parse import urlparse, parse_qs
import rdflib
import requests
import augpathlib as aug
from pyontutils.config import auth as pauth
from pyontutils.core import OntId
from pyontutils.utils import byCol
from sparcur import normalization as nml
from sparcur.core import log, logd, JEncode
from sparcur.paths import Path
#from sparcur.utils import cache
from sparcur.config import config, auth

# ontology files
class OntologyData:

    def _mis_graph(self):
        """ for now easier to just get a fresh one, they are small """
        glb = pauth.get_path('git-local-base')
        olr = Path(glb / 'duplicates' / 'sparc-NIF-Ontology')
        graph = (rdflib.ConjunctiveGraph()
            .parse((olr / 'ttl/sparc-methods.ttl').as_posix(), format='turtle')
            #.parse((olr / 'ttl/methods-core.ttl').as_posix(), format='turtle')
            #.parse((olr / 'ttl/methods-helper.ttl').as_posix(), format='turtle')
            #.parse((olr / 'ttl/methods.ttl').as_posix(), format='turtle')
        )
        return graph

    def run_reasoner(self):
        graph = self._mis_graph()
        expanded_graph = self._mis_graph()
        [(graph.add(t), expanded_graph.add(t))
         for t in self.triples()]
        closure = rdfc.OWLRL_Semantics
        rdfc.DeductiveClosure(closure).expand(expanded_graph)
        with open(auth.get_path('cache-path') / 'reasoned-curation-export.ttl', 'wb') as f:
            f.write(expanded_graph.serialize(format='nifttl'))


# other

class OrganData:
    """ retrieve SPARC investigator data """

    url = ('https://commonfund.nih.gov/sites/default/'
           'files/sparc_nervous_system_graphic/main.html')

    def organ(self, award_number):
        if award_number in self.manual and award_number not in self.sourced:
            log.warning(f'used manual organ mapping for {award_number}')
        try:
            return self.award_to_organ[award_number]
        except KeyError as e:
            logd.error(f'bad award_number {award_number}')

    __call__ = organ

    organ_lookup = {'bladder': OntId('FMA:15900'),
                    'brain': OntId('UBERON:0000955'),
                    #'computer': OntId(''),
                    'heart': OntId('FMA:7088'),
                    'kidneys': OntId('FMA:7203'),
                    'largeintestine': OntId('FMA:7201'),
                    'liver': OntId('FMA:7197'),
                    'lung': OntId('FMA:7195'),
                    'malerepro': OntId('UBERON:0000079'),
                    #'othertargets': OntId(''),
                    'pancreas': OntId('FMA:7198'),
                    'smallintestine': OntId('FMA:7200'),
                    'spleen': OntId('FMA:7196'),
                    'stomach': OntId('FMA:7148'),
                    'vagus nerve': OntId('FMA:5731'),
                    #'uterus': OntId('')
                    '': None,
    }

    cache = auth.get_path('cache-path') / 'sparc-award-by-organ.json'
    old_cache = auth.get_path('cache-path') / 'award-mappings-old-to-new.json'

    def __init__(self, path=config.organ_html_path, organs_sheet=None):  # FIXME bad passing in organs
        from bs4 import BeautifulSoup
        self._BeautifulSoup = BeautifulSoup
        self.path = path
        if not self.cache.exists():
            self.overview()
            with open(self.cache, 'wt') as f:
                json.dump(self.normalized, f)

            with open(self.old_cache, 'wt') as f:
                json.dump(self.former_to_current, f)
        else:
            with open(self.cache, 'rt') as f:
                self.normalized = json.load(f)

            with open(self.old_cache, 'rt') as f:
                self.former_to_current = json.load(f)

        if organs_sheet is not None:
            self._org = organs_sheet
            bc = self._org.byCol
            self.manual  = {award if award else (award_manual if award_manual else None):
                            [OntId(t) for t in organ_term.split(' ') if t]
                            for award, award_manual, organ_term
                            in zip(bc.award, bc.award_manual, bc.organ_term)
                            if organ_term}
        else:
            self.manual = {}

        self.sourced = {v:k for k, vs in self.normalized.items() for v in vs}
        self.award_to_organ = {**self.sourced, **self.manual}  # manual override

    def overview(self):
        if self.path.exists():
            with open(self.path, 'rb') as f:
                soup = self._BeautifulSoup(f.read(), 'lxml')
        else:
            resp = requests.get(self.url)
            soup = self._BeautifulSoup(resp.content, 'lxml')

        self.raw = {}
        self.former_to_current = {}
        for bsoup in soup.find_all('div', {'id':lambda v: v and v.endswith('-bubble')}):
            organ, *_rest = bsoup['id'].split('-')
            logd.debug(_rest)
            award_list = self.raw[organ] = []
            for asoup in bsoup.find_all('a'):
                href = asoup['href']
                log.debug(href)
                parts = urlparse(href)
                query = parse_qs(parts.query)
                if 'projectnumber' in query:
                    award_list.extend(query['projectnumber'])
                elif 'aid' in query:
                    #aid = [int(a) for a in query['aid']]
                    #json = self.reporter(aid)
                    award, former = self.reporter(href)
                    award_list.append(award)
                    if former is not None:
                        award_list.append(former)  # for this usecase this is ok
                        self.former_to_current[former] = award
                elif query:
                    log.debug(lj(query))
            
        self.former_to_current = {nml.NormAward(nml.NormAward(k)):nml.NormAward(nml.NormAward(v))
                                  for k, v in self.former_to_current.items()}
        self._normalized = {}
        self.normalized = {}
        for frm, to in ((self.raw, self._normalized), (self._normalized, self.normalized)):
            for organ, awards in frm.items():
                if organ in self.organ_lookup:
                    organ = self.organ_lookup[organ].iri

                to[organ] = [nml.NormAward(a) for a in awards]

    def _reporter(self, aids):
        # can't seem to get this to cooperate
        base = ('https://api.federalreporter.nih.gov'
                '/v1/projects/FetchBySmApplIds')
        resp = requests.post(base, json=aids, headers={'Accept': 'application/json',
                                                       'Content-Type': 'application/json'})
        breakpoint()
        return resp.json()

    def reporter(self, href):
        resp = requests.get(href)
        soup = self._BeautifulSoup(resp.content, 'lxml')
        #id = soup.find_all('span', {'id': 'spnPNUMB'})
        table = soup.find_all('table', {'summary': 'Details'})
        if table:
            text = table[0].find_all('td')[1].text.strip()
            if 'Former' in text:
                award, rest = text.split(' ', 1)
                rest, former = text.rsplit(' ', 1)
                return [award, former]
            else:
                return [text, None]
        else:
            return ['', None]


class MembersData:
    """ blackfynn members data """
    def __init__(self, blackfynn_local_instance):
        self.__class__._bfli = blackfynn_local_instance

    def __call__(self, first_name, last_name):
        return self.get_member_by_name(first_name, last_name)

    @property
    def members(self):
        if not hasattr(self.__class__, '_members'):
            self.__class__._members = self._bfli.organization.members

        return self._members

    @property
    def __members(self):
        if not hasattr(self.__class__, '_members'):
            log.debug('going to network for members')
            # there are other ways to get here but this one caches
            # e.g. self.organization.path.remote.bfobject
            # self.path.remote.oranization.bfobject
            # self.path.remote.bfl.organization.members
            self.__class__._members = self.path.remote.bfl.organization.members

        return self._members

    @property
    def member_index_f(self):
        if not hasattr(self.__class__, '_member_index_f'):
            mems = defaultdict(lambda:defaultdict(list))
            for member in self.members:
                fn = member.first_name.lower()
                ln = member.last_name.lower()
                current = mems[fn][ln].append(member)

            self.__class__._member_index_f = {fn:dict(lnd) for fn, lnd in mems.items()}

        return self._member_index_f

    @property
    def member_index_l(self):
        if not hasattr(self.__class__, '_member_index_l'):
            mems = defaultdict(dict)
            for fn, lnd in self.member_index_f.items():
                for ln, member_list in lnd.items():
                    mems[ln][fn] = member_list

            self.__class__._member_index_l = dict(mems)

        return self._member_index_l

    def get_member_by_name(self, first, last):
        def lookup(d, one, two):
            if one in d:
                ind = d[one]
                if two in ind:
                    member_list = ind[two]
                    if member_list:
                        member = member_list[0]
                        if len(member_list) > 1:
                            log.critical(f'WE NEED ORCIDS! {one} {two} -> {member_list}')
                            # organization maybe?
                            # or better, check by dataset?

                        return member

        fnd = self.member_index_f
        lnd = self.member_index_l
        fn = first.lower()
        ln = last.lower()
        m = lookup(fnd, fn, ln)
        if not m:
            m = lookup(lnd, ln, fn)

        return m
