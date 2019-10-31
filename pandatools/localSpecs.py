import copy

class LocalTaskSpec(object):

    _attributes_hidden = (
                        '_pandaserver',
                        '_timestamp',
                        '_sourceurl',
                        '_weburl',
                        '_fulldict',
                        )

    _attributes_direct = (
                        'jeditaskid',
                        'reqid',
                        'taskname',
                        'username',
                        'creationdate',
                        'modificationtime',
                        'superstatus',
                        'status',
                        )

    _attributes_dsinfo = (
                        'pctfinished',
                        'pctfailed',
                        'nfiles',
                        'nfilesfinished',
                        'nfilesfailed',
                        )

    __slots__ = _attributes_hidden + _attributes_direct + _attributes_dsinfo

    # stdout string format
    strf_dict = {}
    strf_dict['standard'] = '{jtid:>10}  {reqid:>8}  {sst:>10}/{st:10}  {pctf:>5}  {tname}'
    strf_dict['long'] = (   '{jtid:>10}  {sst:>10}  {cret:20}  {modt:20}  ({filesprog})\n'
                            '{reqid:>10}  {st:>10}  {tname}\n'
                            '            {pctf:>10}  {weburl}\n' ) + '_'*78

    # header row
    head_dict = {}
    head_dict['standard'] = strf_dict['standard'].format(sst='SupStatus', st='Status', jtid='JediTaskID',
                                                            reqid='ReqID', pctf='Fin%', tname='TaskName') \
                            + '\n' + '_'*64
    head_dict['long'] = strf_dict['long'].format(sst='SupStatus', st='Status', jtid='JediTaskID', reqid='ReqID',
                                                tname='TaskName', weburl='Webpage', filesprog='finished|  failed|   total NInputFiles',
                                                pctf='Finished_%', cret='CreationDate', modt='ModificationTime')

    def __init__(self, task_dict, source_url=None, timestamp=None,
                    pandaserver='https://pandaserver.cern.ch:25443/server/panda'):
        self._timestamp = timestamp
        self._sourceurl = source_url
        self._pandaserver = pandaserver
        self._fulldict = copy.deepcopy(task_dict)
        for aname in self._attributes_direct:
            setattr(self, aname, self._fulldict.get(aname))
        for aname in self._attributes_dsinfo:
            if aname.startswith('pct'):
                setattr(self, aname, '{0}%'.format(self._fulldict['dsinfo'][aname]))
            else:
                setattr(self, aname, '{0}'.format(self._fulldict['dsinfo'][aname]))
        self._weburl = 'https://bigpanda.cern.ch/tasknew/{0}/'.format(self.jeditaskid)

    def print_plain(self):
        print('_'*64)
        str_format = '{attr:18} : \t{value}'
        for aname in self.__slots__:
            if aname in ['_fulldict']:
                continue
            print(str_format.format(attr=aname, value= getattr(self, aname)))


    def print_long(self):
        str_format = self.strf_dict['long']
        print(str_format.format(sst=self.superstatus, st=self.status, jtid=self.jeditaskid, reqid=self.reqid,
                                tname=self.taskname, weburl=self._weburl, pctf=self.pctfinished,
                                cret=self.creationdate, modt=self.modificationtime,
                                filesprog='{nff:>8}|{nfb:>8}|{nf:>8}'.format(
                                            nf=self.nfiles, nff=self.nfilesfinished, nfb=self.nfilesfailed)
                                ))

    def print_standard(self):
        str_format = self.strf_dict['standard']
        print(str_format.format(sst=self.superstatus, st=self.status, jtid=self.jeditaskid,
                                reqid=self.reqid, pctf=self.pctfinished, tname=self.taskname))
