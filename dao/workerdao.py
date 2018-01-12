from db.mysqlpython import mySqlPython
import sys


class Worker(object):
    """docstring for Worker"""

    __view_arg = None

    def __init__(self, arg, normal):
        super(Worker, self).__init__()
        self.__view_arg = arg
        self.workername = arg["worker"] if normal else arg
        self.currhashrate = self.get_normalHashRate(arg["currentHashrate"]) if normal else 0
        self.repohashrate = self.get_normalHashRate(arg["reportedHashrate"]) if normal else 0

    # def __get_hashrate(self, hr):
    #     t = re.findall(r"[-+]?\d*\.\d+|\d+", hr)
    #     if not t:
    #     	print("not value. hr: '%s'" % hr)
    #     	return 0
    #     else:
    #     	return float(t[0])

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


class WorkerDao(object):

    """docstring for WorkerDao"""

    __db = None
    __lastid = None
    __sessionid = None

    def __init__(self):
        super(WorkerDao, self).__init__()
        self.__db = mySqlPython()

    def set_worker(self, worker_json, sessionid, normal=True):
    	self.worker = Worker(worker_json, normal)
    	self.__sessionid = sessionid

    def get_worker(self):
        return self.__db.select("worker", None, "worker_name")

    def insert_worker(self):
        self.__lastid = self.__db.insert_ignore_row(
            "worker", worker_name=self.worker.workername)


    def insert_worker_stat(self):
        
        if self.__lastid == 0:
            where_query = 'worker_name = "%s"' % self.worker.workername
            res = self.__db.select("worker", where_query, "id")
            self.__lastid = res[0]

        if self.worker.currhashrate != None:
            self.__db.insert_row("statistics", worker_id=self.__lastid,
                             currHashRate=self.worker.currhashrate,
                             reportHashRate=self.worker.repohashrate,
                             session_id=self.__sessionid)

    def close_conn(self):
        self.__db.close_connection()


    # self.__db.insert_update_row("statistics", worker_id=self.__lastid,
        #                      currHashRate=self.worker.currhashrate,
        #                      reportHashRate=self.worker.repohashrate,
        #                      session_id=self.__sessionid)

    # def compare_value(self):
    #     res = self.__db.select("worker", "worker_name")
        
    #     if not res:
    #         # print("List is empty")
    #         return 0