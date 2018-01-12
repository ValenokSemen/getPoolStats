from db.mysqlpython import mySqlPython
# import dateutil.parser
import datetime


class PayOut(object):
    """docstring for PayOut"""

    def __init__(self, arg):
        super(PayOut, self).__init__()
        self.id = arg['start']
        self.amount = self._get_amount(arg["amount"])
        self.txHash = arg["txHash"]
        self.paidOn = self._get_datetime(arg["paidOn"])

    def _get_amount(self, str):
        return "%0.17f" % (float(str) / (10**18))

    def _get_datetime(self, datetimestr):
        # return dateutil.parser.parse(datetimestr).strftime('%Y-%m-%d %H:%M:%S')
        return datetime.datetime.fromtimestamp(int(datetimestr)).strftime('%Y-%m-%d %H:%M:%S')


class PayoutDao(object):

    __db = None

    """docstring for PayOutDao"""

    def __init__(self, payout_json):
        super(PayoutDao, self).__init__()
        self.__db = mySqlPython()
        self.payout = PayOut(payout_json)

    def insert_payout(self):
        self.__db.insert_update_row("payout",
                                    id=self.payout.id,
                                    amount=self.payout.amount,
                                    txHash=self.payout.txHash,
                                    paidDate=self.payout.paidOn)

    def insert_paidstatus(self, minerlist):
        for val in minerlist:
            self.__db.insert_update_row(
                "paidstatus", worker_name=val, payout=self.payout.id)

    def close_conn(self):
        self.__db.close_connection()
