from db.mysqlpython import mySqlPython


class Miner(object):
    """docstring for Miner"""

    def __init__(self, arg):
        super(Miner, self).__init__()
        self.__view_arg = arg
        self.unpaid = self.get_unpaidbalance(arg['data']['unpaid'])
        self.averageHashrate = self.get_normalHashRate(
            arg['data']["averageHashrate"])
        self.reportedHashrate = self.get_normalHashRate(
            arg['data']["reportedHashrate"])
        self.currentHashrate = self.get_normalHashRate(
            arg['data']["currentHashrate"])
        self.coinsPerMin = arg['data']['coinsPerMin']

    def get_normalHashRate(self, hashrate):
        raised = True
        val  = None
        try:
            if hashrate is None:
                val = None;
            else:
                val =  round(float(hashrate) / pow(10, 6), 1)
        except TypeError:
            print("The operation is applied to an object of an inappropriate type")
            print("hashrate: ", hashrate)
            print("json_arg: ", self.__view_arg)
        else:
            raised = False # if no error was raised
        finally:
            if raised:
                raise SystemExit
            else:
                return val   

    def get_unpaidbalance(self, unpaid):
        raised = True
        val  = None
        try:
            if unpaid is None:
                val = 0;
            else:
                val =  "%0.5f" % (float(unpaid) / (10**18))
        except TypeError:
            print("The operation is applied to an object of an inappropriate type")
            print("unpaid: ", unpaid)
            print("json_arg: ", self.__view_arg)
        else:
            raised = False # if no error was raised
        finally:
            if raised:
                raise SystemExit
            else:
                return val


class MinerDao(object):
    """docstring for MinerDao"""

    def __init__(self, _json):
        super(MinerDao, self).__init__()
        self.__db = mySqlPython()
        self.miner = Miner(_json)


    def insert_mining(self):      
        return self.__db.insert_row("miner",
                         repreportedHashrate=self.miner.reportedHashrate,
                         currentHashrate=self.miner.currentHashrate,
                         averageHashrate=self.miner.averageHashrate,
                         balance=self.miner.unpaid,
                         ethPerMin=self.miner.coinsPerMin)

    def close_conn(self):
    	self.__db.close_connection()
