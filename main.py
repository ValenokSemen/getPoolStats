# !/usr/bin/python
import schedule
import functools
import sys
import time

from configfile import config
from dao.minerdao import MinerDao
from dao.payoutdao import PayoutDao
from dao.workerdao import WorkerDao

from etherminepl import Eterminepl



class App(object):

    __appstat = config["app"]
    __json = None
    json_err = False

    # miner_stat = dict(balance="0", chashrate="0", ahashrate="0")

    """docstring for App"""

    def __init__(self):
        super(App, self).__init__()
        # self.__json = self.get_json_from_file()
        self.ethpool = Eterminepl(self.__appstat["wallet"])
        self.ethpool.get_all_response()
        if self.ethpool.get_response_status() == False:
            self.json_err = True
        else:
            self.json_err = False

        # if self.get_jsondata() == 1:
        #     print("Could not fetch JSON from host")

    def get_json_err(self):
        return self.json_err

    def insert_in_worker(self, sessionid):
        __json_workername = []
        _json = self.ethpool.get_workers()
        for val in _json['data']:
            __json_workername.append(val['worker'])

        _workerDao = WorkerDao()

        __db_workername = _workerDao.get_worker()
        __res = self.returnNotMatches(__db_workername, __json_workername)

        for val in _json['data']:
            _workerDao.set_worker(val, sessionid)
            _workerDao.insert_worker()
            _workerDao.insert_worker_stat()

        if __res[0] > __res[1]:
            for val in __res[0]:
                _workerDao.set_worker(val, sessionid, False)
                _workerDao.insert_worker()
                _workerDao.insert_worker_stat()

        if not __res[0]:
            r = self.returnNotMatches(
                config["app"]["workers"], __json_workername)
            for val in r[0]:
                _workerDao.set_worker(val, sessionid, False)
                _workerDao.insert_worker()
                _workerDao.insert_worker_stat()

        _workerDao.close_conn()

    def insert_in_payout(self):
        _json = self.ethpool.get_payouts()
        for val in _json['data']:
            _payoutdao = PayoutDao(val)
            _payoutdao.insert_payout()
            _payoutdao.insert_paidstatus(config["app"]["workers"])
            _payoutdao.close_conn()

    def insert_in_minerstat(self):
        _json = self.ethpool.get_currentStats()
        _minerdao = MinerDao(_json)
        sessionid = _minerdao.insert_mining()
        _minerdao.close_conn()
        return sessionid

    def returnNotMatches(self, a, b):
        return [[x for x in a if x not in b], [x for x in b if x not in a]]

    # def get_json_from_file(self):
    #     with open('tmp.json') as f:
    #         return js.load(f)


# This decorator can be applied to
def with_logging(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print('LOG: Running job "%s"' % func.__name__)
        result = func(*args, **kwargs)
        print('LOG: Job "%s" completed' % func.__name__)
        return result
    return wrapper


@with_logging
def job():
    work = App()
    if work.get_json_err():
        print("Could not fetch JSON from host")       
    else:
        sessionid = work.insert_in_minerstat()
        work.insert_in_worker(sessionid)
        work.insert_in_payout()
        print("row insert: ", time.ctime())
        


if __name__ == '__main__':
    try:
        print("start: %s" % time.ctime())
        schedule.every(config["app"]["interval"]).minutes.do(job)
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt as e:
        sys.exit(0)

# if __name__ == '__main__':
#     try:
#         work = App()
#         if work.get_json_err():
#             print("Could not fetch JSON from host")       
#         else:
#             sessionid = work.insert_in_minerstat()
#             work.insert_in_worker(sessionid)
#             work.insert_in_payout()
#             print("row insert: ", time.ctime())
#     	# Exit cleanly on ctl-c
#     except KeyboardInterrupt as e:
#     	sys.exit(0)
