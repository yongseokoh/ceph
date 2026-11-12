from .analyzer import JobState
from .mover import MoverCtxState
from enum import Enum, unique
import logging
import threading
import time
import json

log = logging.getLogger(__name__)

@unique
class SchedulerState(Enum):
    INITIALIZED          = 0
    INVOKED              = 1
    RUNNING_ANALYZER     = 2
    RUNNING_MOVER        = 3
    WAIT                 = 4
    STOP                 = 5

class Scheduler:
    def __init__(self, analyzer, mover, policy_mgr):
        self.analyzer = analyzer
        self.mover = mover
        self.policy_mgr = policy_mgr
        self.thread = None
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.state = SchedulerState.INITIALIZED
        self.scheduler_period = 0
        self.analyzer_period = 0
        self.policy_name = ''

    def start(self, policy_name, analyzer_period, scheduler_period):
        log.debug('Scheduler start')
        with self.lock:
            res, msg  = self.policy_mgr.show(policy_name)
            if res == False:
                return False, msg

            if self.state not in [SchedulerState.INITIALIZED, SchedulerState.STOP]:
                return False, f'Scheduler is in {self.state.name}'

            if analyzer_period <= 0:
                return False, f'analyhzer_period should be greater than 0'

            if scheduler_period <= 0:
                return False, f'scheduler_period should be greater than 0'

            self.analyzer_period = analyzer_period
            self.scheduler_period = scheduler_period
            self.policy_name = policy_name
            self.stop_event = threading.Event()
            self.thread = threading.Thread(target=self.run)
            self.state = SchedulerState.INVOKED
            self.thread.start()
            return True, f"Scheduler for {policy_name} has been invoked."

    def stop(self):
        log.debug('Scheduler stop')
        with self.lock:
            self.stop_event.set()
            thread = self.thread
        if thread is not None:
            thread.join()

    def wait_next(self):
        self.stop_event.wait(self.scheduler_period)
        return True

    def status(self, output_format='json'):
        with self.lock:
            dict_data = {
                            'state': self.state.name,
                            'scheduler_period': self.scheduler_period,
                            'analyzer_period': self.analyzer_period,
                            'policy_name': self.policy_name
                        }
            if output_format == 'dict':
                return dict_data
            return json.dumps(dict_data, indent=4)

    def run_analyzer(self):
        res, msg = self.analyzer.start(self.policy_name, self.analyzer_period, 1)
        if res != True:
            log.debug(f'Scheduler anayzler error in scheduler: {msg}')
            return res

        log.debug("analyzer running")
        start_time = time.monotonic()
        try:
            while not self.stop_event.is_set():
                res, data = self.policy_mgr.show(self.policy_name, output_format='dict')
                if not res:
                    return False
                elapsed_time = time.monotonic() - start_time
                if data['workload_table_ready'] == 'True' and elapsed_time >= self.analyzer_period:
                    return True
                if self.analyzer.status()['timer_state'] == JobState.DONE.name:
                    return data['workload_table_ready'] == 'True'
                self.stop_event.wait(1)
            return False
        finally:
            self.analyzer.stop()

    def run_mover(self):
        res, msg = self.mover.start(self.policy_name)
        if res != True:
            log.debug(f'Mover start error in scheduler: {msg}')
            return False

        log.debug("mover start")

        try:
            while not self.stop_event.is_set():
                status = self.mover.status('dict')
                if status['status'] == MoverCtxState.FAILED.name:
                    return False
                if status['status'] == MoverCtxState.DONE.name:
                    return status['total_jobs'] == status['processed_jobs']
                self.stop_event.wait(1)
            return False
        finally:
            self.mover.stop()

    def run(self):
        log.debug('Scheduler run')

        with self.lock:
            self.state = SchedulerState.RUNNING_ANALYZER

        try:
            while not self.stop_event.is_set():
                with self.lock:
                    state = self.state

                log.debug(f"Scheduler worker thread is running in {state}.")

                if state == SchedulerState.RUNNING_ANALYZER:
                    res = self.run_analyzer()
                elif state == SchedulerState.RUNNING_MOVER:
                    res = self.run_mover()
                else:
                    res = self.wait_next()

                with self.lock:
                    if not res:
                        self.state = SchedulerState.WAIT
                    elif state == SchedulerState.RUNNING_ANALYZER:
                        self.state = SchedulerState.RUNNING_MOVER
                    elif state == SchedulerState.RUNNING_MOVER:
                        self.state = SchedulerState.WAIT
                    else:
                        self.state = SchedulerState.RUNNING_ANALYZER
        finally:
            try:
                self.analyzer.stop()
            finally:
                try:
                    self.mover.stop()
                finally:
                    with self.lock:
                        self.state = SchedulerState.STOP
