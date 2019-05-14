from sparcur import sheets
from sparcur import datasources as ds
#from sparcur import protocols

# state downstream of static external sources


class State:
    """ stateful values that many things need to access after startup
        that are beyond just the command line interface and which we
        don't want to continually create new versions of, in practice
        static information should flow from here rather than being set
        somewhere else and simply dumped here
    """

    @classmethod
    def bind_blackfynn(cls, blackfynn_local_instance):
        # FIXME bfli should flow from here out not the other way around
        # however there are some use cases, such as merging between
        # different organizations where you don't want to for the rest
        # of the program to be stuck with a single source, however for
        # our purposes here, we do need a way to say 'one at a time please'
        cls.blackfynn_local_instance = blackfynn_local_instance
        cls.member = ds.MembersData(blackfynn_local_instance)

    @classmethod
    def bind_protocol(cls, protocol_data):
        cls.protocol = protocol_data
