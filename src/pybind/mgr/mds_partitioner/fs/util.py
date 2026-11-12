import os
import errno
import threading
import queue
from .exception import MDSPartException
import logging
from prettytable import PrettyTable, ALL

log = logging.getLogger(__name__)

def get_max_mds(fsmap, fs_name):
    for fs in fsmap['filesystems']:
        if fs['mdsmap']['fs_name'] == fs_name:
            return fs['mdsmap']['max_mds']
    log.error(f'fs_name {fs_name} is not found in fsmap.')
    assert False

def get_bal_rank_mask(fsmap, fs_name):
    for fs in fsmap['filesystems']:
        if fs['mdsmap']['fs_name'] == fs_name:
            return fs['mdsmap']['bal_rank_mask']
    log.error(f'fs_name {fs_name} is not found in fsmap.')
    assert False

def set_bal_rank_mask(mgr, fs_name, mask):
    cmd = {'prefix': 'fs set', 
           'fs_name': fs_name,
           'var': 'bal_rank_mask', 
           'val': str(mask),
           'format': 'json'}
    try:
        ret, outs, err = mgr.mon_command(cmd)
    except Exception as e:
        raise MDSPartException(f'error in \'fs set\' query: {e}')
    if ret != 0:
        raise MDSPartException(f'error in \'fs set\' query: {err}')

def dict_to_prettytable(dict_data, header=False):
    table = PrettyTable(['k', 'v'])
    for key, value in dict_data.items():
        table.add_row([key, value])
    table.hrules = ALL
    return table.get_string(header=header)

def norm_path(dir_path):
    if not os.path.isabs(dir_path):
        raise MDSPartException(-errno.EINVAL, f'{dir_path} should be an absolute path')
    return os.path.normpath(dir_path)

class WorkQueue(threading.Thread):
    def __init__(self, name):
        self.queue: queue.Queue = queue.Queue()
        self.stopping = threading.Event()
        self.terminated = threading.Event()
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        super().__init__(name=name)
        super().start()

    def run(self):
        log.debug('workqueue run')
        try:
            while True:
                with self.lock:
                    self.cond.wait_for(lambda: not self.queue.empty() or \
                                                   self.stopping.is_set())

                    if self.stopping.is_set():
                        return

                    context = self.queue.get()
                try:
                    context.run()
                except Exception:
                    log.exception('workqueue callback failed')
        finally:
            self.terminated.set()

    def enqueue(self, context):
        with self.lock:
            if self.stopping.is_set():
                raise RuntimeError('workqueue has stopped')
            self.queue.put(context)
            self.cond.notify_all()

    def stop(self):
        with self.lock:
            self.stopping.set()
            self.cond.notify_all()
        self.join()
