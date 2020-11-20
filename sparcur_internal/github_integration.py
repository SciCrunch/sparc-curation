#!/usr/bin/env python3

from types import MappingProxyType
import yaml
import idlib
from github import Github
from sparcur import exceptions as exc
from sparcur.config import auth
from sparcur.core import adops
from sparcur.utils import log
from sparcur.export import latest_ir
install_requires = ['PyGithub']


def condense(thing, delist=True):
    if isinstance(thing, dict):
        return {k:nv for k, nv in [(k, condense(v)) for k, v in thing.items()]
                if nv}
    elif isinstance(thing, list):
        out = [_ for _ in [condense(t) for t in thing] if _]
        if delist and len(out) == 1:
            return out[0]
            
        return out
    else:
        return thing


def _issubclass(thing, cls):
    """ LOL PYTHON """
    # why would you raise a type error instead of returning false !??!?!
    try:
        return issubclass(thing, cls)
    except TypeError:
        return False


class DictObject:
    """ static accessor for dictionary objects

        operators
        / any-key
        * over-list -> [['further', 'path', 'elements']]
    """

    _mapping = MappingProxyType({})

    # TODO consider generating these from the json schema or mapping the classes via
    # the schema ... the convention I use here is a bit of a pain to create

    def __new__(cls, blob):
        cls = type(cls.__name__ + '_DictObject', (cls,), {})
        for attr, path in cls._mapping.items():  # FIXME consider doing path -> attr instead so the could be combined
            term_list = path[-1] if isinstance(path[-1], list) else None
            term = (path[-1]
                    if _issubclass(path[-1], DictObject)
                    else ((path[-1][0] if
                           _issubclass(path[-1][0], DictObject) else
                           None) if
                          term_list else
                          None))

            #log.debug(term)
            if term_list or term:
                path = path[:-1]

            #@property
            #@idlib.utils.cache_result  # FIXME this does NOT work because the function name is always f
            def f(self, path=path, terminal=term, term_list=term_list):
                try:
                    end = adops.get(self._blob, path)  # FIXME / and * would need to be implemented on top of this
                except exc.NoSourcePathError:
                    if term_list:
                        return []
                    elif terminal is not None:
                        return terminal({})
                    else:
                        return []  # FIXME was return None but that breaks missing keys that should contain lists

                # FIXME type check
                if terminal and term_list:
                    return [terminal(o) for o in end]
                elif terminal:
                    return terminal(end)
                elif term_list:
                    raise NotImplementedError('hrm')
                else:
                    return end


            setattr(f, '__name__', attr)

            setattr(cls, attr, property(idlib.utils.cache_result(f)))

        return object.__new__(cls)

    def __init__(self, blob):
        self._blob = blob


class Meta(DictObject):

    _mapping = {
        'uri_human': ['uri_human'],
        'uri_api': ['uri_api'],
        '_award_number': ['award_number'],
        'folder_name': ['folder_name'],
        # FIXME need to figure out how to preserve the type :/
        'protocols': ['protocol_url_or_doi'],  # FIXME merge multiple paths ? or collect from subjects to meta as well?
        'created': ['timestamp_created'],
    }

    @property
    @idlib.utils.cache_result
    def award_number(self):
        an  = self._award_number
        if an:
            return Award(an)


class Contributor(DictObject):

    _mapping = {
        'id': ['id'],
        'first_name': ['first_name'],
        'last_name': ['last_name'],
        'orcid': ['contributor_orcid_id'],
        'role': ['contributor_role'],
        'contact': ['is_contact_person'],
        }


class Dataset(DictObject):

    # FIXME allowing the mapping to go only a single path deep
    # would mean that we wouldn't have to explicitly create
    # a mapping for all the keys, but could inspect the blob to generate
    # any that were not specified
    # XXX ON THE OTHER HAND: if you start using d.meta.value and d.meta is None
    # you are in a world of pain :/ SIGH
    _mapping = {
        'id': ['id'],
        'meta': ['meta', Meta],
        'contributors': ['contributors', [Contributor]],  # FIXME probably need to spec the internal type
        #'uri_human': ['meta', 'uri_human'],
        #'uri_api': ['meta', 'uri_api'],
        #'award_number': ['meta', 'award_number'],
        #'folder_name': ['meta', 'folder_name'],
        #'protocols': ['meta', 'protocol_url_or_doi'],  # FIXME merge multiple paths ? or collect from subjects to meta as well?
    }

    def asIssue(self, repo):
        return DatasetIssue(self, repo)


class OtAwardId(idlib.Identifier, idlib.Stream):

    _local_conventions = idlib.conventions.Identity

    def __init__(self, identifier):
        self._identifier = identifier  # FIXME if this is a list or something other than a string, __str__ breaks below

    def __str__(self):
        id = self.identifier
        if not isinstance(id, str):
            raise TypeError(id)

        return id

    def __repr__(self):
        return self.__class__.__name__ + f"({self.identifier!r})"


class Award(idlib.Stream):
    # FIXME TODO and here we see why we want to have resolvers as their own thing
    # multiple award ids would make use of a single stream type
    # in this case it is the "scicrunch reporter metadata stream"
    # and there can and will be many more than one _id_class that could
    # be associated with this stream/resolver, the question is how
    # to handle that non-1:1-ness
    _id_class = OtAwardId  # FIXME ... _id_classes ? and then set _id_class @ instance level?
    _resolver_template = ('https://scicrunch.org/scicrunch/data/source/'
                          'nif-0000-10319-1/search?q={id}')
    identifier_actionable = idlib.StreamUri.identifier_actionable
    dereference_chain = idlib.StreamUri.dereference_chain
    dereference = idlib.StreamUri.dereference
    headers = idlib.StreamUri.headers

    def __init__(self, award_number):
        self._identifier = self._id_class(award_number)

    def scicrunchUri(self, asType=None):
        uri_string = self._resolver_template.format(id=self.identifier)
        return uri_string if asType is None else asType(uri_string)


class GithubRepo(idlib.Stream):
    # substreams
    # git repo
    # wiki git repo
    # issues
    # other api stuff

    # XXX GithubRepo isn't really a Stream because it is not
    # in principle immutable in the way we want/need streams to be
    # things can change rapidly and thus synchronization of state
    # has to happen much more frequently than with streams ...
    # maybe idlib.Dynamic ? or idlib.Storm ? or idlib.River ?
    # idlib.Well idlib.Pool idlib.Creek idlib.Sewer idlib.Whirlpool
    # idlib.Sea idlib.Cloud (lol) idlib.Rain idlib.Tsunami ...
    # need more dynamic water terms ... idlib.Flood

    def __init__(self, fragment):
        self._identifier = fragment
        # FIXME orthauth this and defer setup
        github = Github(auth.user_config.secrets('github',
                                                 'tgbugs-build',
                                                 'WARNING-development-all-scopes'))
        #repo = github.get_repo('SciCrunch/sparc-datasets')
        self.repo = github.get_repo(self._identifier)
        self._issues = []

    @property
    def issues(self):
        if not self._issues:
            self._pull_issues()

        return self._issues

    def _pull_issues(self):
        self._issues = self.repo.get_issues()


class GithubIssue(idlib.Stream):
    # TODO set tags
    pass


class IssueMapper:
    """ Base class for mapping external ids to their corresponding
        issue tracker identifier. """


class DatasetIssue(IssueMapper):
    """ class that maps datasets to their github issue """

    def __init__(self, dataset, repo):
        # TODO path vs blob vs id + latest
        self.dataset = dataset
        self.repo = repo  # should be a GithubRepo not a github.Repo
        existing = self.exists()
        if existing:  # FIXME catch multiple issue
            self._issue = existing[0]

    @property
    def dataset_id(self):
        return self.dataset.id

    @property
    def issue_id(self):
        # TODO uri_human = self._issue.html_url
        return self._issue.number

    @property
    def new_issue_title(self):
        return f'Tracking {self.dataset_id}'

    def parse_top_comment(self):
        """ needed for updates """

    @staticmethod
    def _markdown_link(href, text=None):
        if text is not None:
            return f'[{text}]({href})'
        else:
            return href

    @property
    def dataset_link(self):
        try:
            return self._markdown_link(self.dataset.meta.uri_human, self.dataset.id)
        except AttributeError as e:
            breakpoint()
            raise e

    @property
    def dataset_award(self):
        if self.dataset.meta.award_number:
            id = self.dataset.meta.award_number.identifier.identifier
            if id:
                return (f'{id} '
                        f'{self._markdown_link(self.dataset.meta.award_number.scicrunchUri(), "SciCrunch")}')

    @property
    def dataset_protocols(self):
        return [self._markdown_link(p.identifier, p.label_safe) for p in self.dataset.meta.protocols]

    @property
    def dataset_doi(self):
        if hasattr(self.dataset.meta, 'doi') and self.dataset.meta.doi:
            doi = self.dataset.meta.doi
            return f'{doi}'

    def new_issue_blob(self):
        a, b = self.new_issue_blobs()
        return {**a, **b}

    def _contrib_fmt(self, c, flag=True):
        bad = '' if 'orcid.org' in c.id or not flag else '*'
        # FIXME for some reason c.id are strings after loading ir?
        return self._markdown_link(c.id, f'{c.first_name} {c.last_name}{bad}')

    def new_issue_blobs(self):
        pis = [c for c in self.dataset.contributors
               if 'PrincipalInvestigator' in c.role]
        blob_a = {
            'Dataset': self.dataset_link,
            'Title': self.dataset.meta.folder_name,
            'Award': self.dataset_award,
            'Lab': [p.last_name for p in pis][:1],  # FIXME last name also lab might not be the first PI last name
            'People': {
                'PI': [self._contrib_fmt(p) for p in pis],
                'Contact': [self._contrib_fmt(c) for c in self.dataset.contributors
                            if ('Contact' in c.role or c.contact) and c not in pis],
            },
            'Protocols': self.dataset_protocols,
            'Created': self.dataset.meta.created,
            'Published': self.dataset_doi,
        }

        blob_b = {
            'isAbout': self.dataset.meta.uri_api,  # FIXME TODO how to embed/rediscover source ids
            # TODO consider embedding the github issue id in the rmeta somehow/somewhere
        }
        return condense(blob_a), condense(blob_b)

    def new_issue_comment(self):
        # FIXME justification
        # NOTE we retain the quotes around hyperlinks to simplify roundtrip
        blobs = self.new_issue_blobs()
        one, two = [yaml.dump(_, default_flow_style=False, sort_keys=False)
                    for _ in blobs]
        return one + '\n----------------\n' + two

    @property
    def new_issue_labels(self):
        return []

    def exists(self):
        title = self.new_issue_title
        self.repo._pull_issues
        return [i for i in self.repo.issues if title == i.title]

    def issue_update(self):
        """update issue from dataset"""
        body = self.new_issue_comment()
        self._issue.edit(body=body)

    def new_issue_create(self):
        if self.exists():
            raise ValueError('Issue already exists! {self.uri_human}')
        #resp = self.submit_new_issue(data)
        title = self.new_issue_title
        body = self.new_issue_comment()
        labels = self.new_issue_labels
        issue = self.repo.repo.create_issue(title=title,
                                            body=body,
                                            labels=labels)
        self._issue = issue
        return issue

    def embed_id_tracker(self):
        """ embed the tracker id in the source metadata somehow """
        if self.id_tracker is not None:
            self.stream


class ProtocolIssue(IssueMapper):
    """ TODO """
    # facet link
    # etc.


def main():
    #print(comment)
    import orthauth as oa

    github = Github(auth.user_config.secrets('github',
                                             'tgbugs-build',
                                             'WARNING-development-all-scopes'))

    grepo = github.get_repo('SciCrunch/sparc-datasets')

    commits = list(grepo.get_commits())
    blob = oa.utils.QuietDict(latest_ir())
    ds = [Dataset(d) for d in blob['datasets']]
    [print(DatasetIssue(d).new_issue_comment()) for d in ds]
    breakpoint()


if __name__ == '__main__':
    main()
