import json
from pyontutils.core import OntId
from sparcur.core import log
from sparcur.paths import Path
from sparcur.config import organ_html_path
from bs4 import BeautifulSoup


class OrganData:
    """ retrieve SPARC investigator data """
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
                    #'uterus': OntId('')
    }
    cache = Path('/tmp/sparc-award-by-organ.json')
    old_cache = Path('/tmp/award-mappings-old-to-new.json')

    def __init__(self, path=organ_html_path):
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

    def overview(self):
        with open(self.path, 'rb') as f:
            soup = BeautifulSoup(f.read(), 'lxml')

        self.raw = {}
        self.former_to_current = {}
        for bsoup in soup.find_all('div', {'id':lambda v: v and v.endswith('-bubble')}):
            organ, _ = bsoup['id'].split('-')
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
        soup = BeautifulSoup(resp.content, 'lxml')
        #id = soup.find_all('span', {'id': 'spnPNUMB'})
        table = soup.find_all('table', {'summary': 'Details'})
        text = table[0].find_all('td')[1].text.strip()
        if 'Former' in text:
            award, rest = text.split(' ', 1)
            rest, former = text.rsplit(' ', 1)
            return [award, former]
        else:
            return [text, None]
