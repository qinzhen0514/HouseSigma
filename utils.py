import threading
from queue import Queue


def getproxy():
    # username = 'xxx'
    # password = 'xxx'
    # port = 'xxx'
    # proxy_url = 'xxx'
    # 
    # proxies = {
    #     "http": proxy_url,
    #     "https": proxy_url,
    # }
    # return proxies
    pass


class Worker(threading.Thread):
    def __init__(self, queue):
        super(Worker, self).__init__()
        self._q = queue
        self.daemon = True
        self.start()

    def run(self):
        while 1:
            f, args, kwargs = self._q.get()
            try:
                # print("USE: {}".format(self.name)) 
                f(*args, **kwargs)
            except Exception as e:
                print(e)
            self._q.task_done()


class ThreadPool(object):
    def __init__(self, num_t=5):
        self._q = Queue(num_t)
        # Create Worker Thread
        for _ in range(num_t):
            Worker(self._q)

    def add_task(self, f, *args, **kwargs):
        self._q.put((f, args, kwargs))

    def wait_complete(self):
        self._q.join()