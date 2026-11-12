import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from mds_partitioner.fs.analyzer import Analyzer, AnalyzerState, JobState
from mds_partitioner.fs.exception import MDSPartException
from mds_partitioner.fs.mds_partition import FSMDSPartition, PartContext
from mds_partitioner.fs.mover import Mover, MoverContext, MoverCtxState
from mds_partitioner.fs.scheduler import Scheduler, SchedulerState
from mds_partitioner.fs.util import WorkQueue
from mds_partitioner.fs.workload_table import MoveType, SubdirState


JOB = ('cephfs', '/subtree', 2, '0x4', MoveType.PIN.name)


def test_stop_during_move_does_not_hold_progress_lock():
    entered = threading.Event()
    release = threading.Event()

    def move(*args):
        entered.set()
        assert release.wait(2)

    context = MoverContext('cephfs', move, Mock())
    context.add_jobs([JOB])
    context.set_state(MoverCtxState.READY)
    worker = threading.Thread(target=context.run, daemon=True)
    stopper = threading.Thread(target=context.stop, daemon=True)
    worker.start()
    try:
        assert entered.wait(2)
        stopper.start()
        assert context.stopping.wait(2)
    finally:
        release.set()
        worker.join(2)
        if stopper.ident is not None:
            stopper.join(2)
    assert not worker.is_alive()
    assert not stopper.is_alive()
    assert context.processed_jobs == 1
    assert context.terminated.is_set()


@pytest.mark.parametrize('fail_history', [False, True])
def test_move_exception_signals_completion(fail_history):
    policy_mgr = Mock()
    callback = Mock()
    if fail_history:
        policy_mgr.freeze_history.side_effect = RuntimeError('history failed')
    else:
        callback.side_effect = RuntimeError('move failed')
    context = MoverContext('cephfs', callback, policy_mgr)
    context.add_jobs([JOB])
    with pytest.raises(RuntimeError):
        context.run()
    context.stop()
    assert context.terminated.is_set()
    assert context.state == MoverCtxState.FAILED
    if not fail_history:
        policy_mgr.update_subtree_state.assert_not_called()
        assert context.processed_jobs == 0


@pytest.fixture
def mover():
    policy_mgr = Mock()
    policy_mgr.get_df.return_value = pd.DataFrame([{
        'subtree': '/subtree', 'rank': 2, 'rank_mask': '0x4',
        'move_type': MoveType.PIN.name, 'state': SubdirState.NEED_TO_MOVE.name,
    }])
    worker = Mover(Mock(), Mock(), Mock(), 'cephfs', policy_mgr)
    try:
        yield worker
    finally:
        worker.close()


def test_each_move_run_has_fresh_context(mover):
    mover._move_subtree = Mock()
    contexts = []
    for _ in range(2):
        assert mover.start('policy')[0]
        context = mover.context
        contexts.append(context)
        assert context.terminated.wait(2)
        mover.stop()
        assert context.total_jobs == context.processed_jobs == 1
        assert context.job_queue.empty()
    assert contexts[0] is not contexts[1]
    mover.close()
    assert not mover.wq.is_alive()


def test_failed_move_can_be_retried(mover):
    mover._move_subtree = Mock(side_effect=[RuntimeError('move failed'), None])
    assert mover.start('policy')[0]
    assert mover.context.terminated.wait(2)
    assert mover.context.state == MoverCtxState.FAILED
    assert mover.start('policy')[0]
    assert mover.context.terminated.wait(2)
    assert mover.context.state == MoverCtxState.DONE


def test_move_passes_rank_and_wait_options(mover):
    mover._set_and_verify_vxattr = Mock(return_value=True)
    mover._wait_subtree = Mock()
    mover._move_subtree('cephfs', '/subtree', 2, '0x4', MoveType.PIN.name,
                        check_wait=3, timeout=30)
    mover._wait_subtree.assert_called_once_with(
        'cephfs', '/subtree', 2, check_wait=3, timeout=30, check_pin=True)


def test_mask_job_does_not_wait_for_unpinned_rank(mover):
    mover._set_and_verify_vxattr = Mock(return_value=True)
    mover._wait_subtree = Mock()
    context = MoverContext('cephfs', mover._move_subtree, mover.policy_mgr)
    context.set_policy('policy')
    context.add_jobs([('cephfs', '/subtree', -1, '0xc', MoveType.CEPH_DIR_BAL_MASK.name)])
    context.run()
    mover._wait_subtree.assert_not_called()
    assert [call.args for call in mover._set_and_verify_vxattr.call_args_list] == [
        ('cephfs', '/subtree', 'ceph.dir.pin', '-1'),
        ('cephfs', '/subtree', 'ceph.dir.bal.mask', '0xc'),
    ]
    assert context.processed_jobs == 1


@pytest.mark.parametrize('results', [[False], [True, False]])
def test_failed_xattr_is_not_recorded_as_success(mover, results):
    mover._set_and_verify_vxattr = Mock(side_effect=results)
    context = MoverContext('cephfs', mover._move_subtree, mover.policy_mgr)
    context.add_jobs([('cephfs', '/subtree', 2, '0x4', MoveType.CEPH_DIR_BAL_MASK.name)])
    with pytest.raises(MDSPartException):
        context.run()
    assert context.processed_jobs == 0
    mover.policy_mgr.update_subtree_state.assert_not_called()


def test_workqueue_survives_failed_callback_and_closes():
    queue = WorkQueue('test-mover')
    completed = threading.Event()
    failed = Mock()
    failed.run.side_effect = RuntimeError('move failed')
    next_job = Mock()
    next_job.run.side_effect = completed.set
    try:
        queue.enqueue(failed)
        queue.enqueue(next_job)
        assert completed.wait(2)
    finally:
        queue.stop()
    assert queue.terminated.is_set()
    assert not queue.is_alive()
    with pytest.raises(RuntimeError):
        queue.enqueue(next_job)


def make_scheduler():
    policy_mgr = Mock()
    policy_mgr.show.return_value = (True, {'workload_table_ready': 'False'})
    return Scheduler(Mock(), Mock(), policy_mgr)


def test_scheduler_initial_status_and_stop():
    scheduler = make_scheduler()
    assert scheduler.status('dict')['state'] == SchedulerState.INITIALIZED.name
    scheduler.stop()


def test_scheduler_failure_transitions_to_wait():
    scheduler = make_scheduler()
    scheduler.run_analyzer = Mock(return_value=False)

    def wait_next():
        assert scheduler.state == SchedulerState.WAIT
        scheduler.stop_event.set()
        return True

    scheduler.wait_next = Mock(side_effect=wait_next)
    scheduler.run()
    scheduler.run_analyzer.assert_called_once()
    scheduler.wait_next.assert_called_once()
    assert scheduler.state == SchedulerState.STOP


def test_scheduler_stop_joins_before_returning():
    scheduler = make_scheduler()
    entered = threading.Event()

    def run_analyzer():
        entered.set()
        scheduler.stop_event.wait(2)
        return False

    scheduler.run_analyzer = run_analyzer
    assert scheduler.start('policy', 10, 10)[0]
    try:
        assert entered.wait(2)
    finally:
        scheduler.stop()
    assert not scheduler.thread.is_alive()
    assert scheduler.state == SchedulerState.STOP
    scheduler.analyzer.stop.assert_called_once()
    scheduler.mover.stop.assert_called_once()


def test_scheduler_exception_still_cleans_up():
    scheduler = make_scheduler()
    scheduler.run_analyzer = Mock(side_effect=RuntimeError('analysis failed'))
    with pytest.raises(RuntimeError):
        scheduler.run()
    assert scheduler.state == SchedulerState.STOP
    scheduler.analyzer.stop.assert_called_once()
    scheduler.mover.stop.assert_called_once()


def test_scheduler_stops_expired_analysis_without_ready_data():
    scheduler = make_scheduler()
    scheduler.policy_name = 'policy'
    scheduler.analyzer_period = 3600
    scheduler.analyzer.start.return_value = (True, 'started')
    scheduler.analyzer.status.return_value = {'timer_state': JobState.DONE.name}
    assert scheduler.run_analyzer() is False
    scheduler.analyzer.start.assert_called_once_with('policy', 3600, 1)
    scheduler.analyzer.stop.assert_called_once()


def test_scheduler_detects_failed_move():
    scheduler = make_scheduler()
    scheduler.mover.start.return_value = (True, 'started')
    scheduler.mover.status.return_value = {'status': MoverCtxState.FAILED.name}
    assert scheduler.run_mover() is False
    scheduler.mover.stop.assert_called_once()


@pytest.mark.parametrize('timer_state', list(JobState))
def test_analyzer_stops_every_timer_state(timer_state):
    analyzer = Analyzer(Mock(), Mock(), Mock(), 'cephfs')
    with patch('mds_partitioner.fs.analyzer.RTimer') as timer:
        assert analyzer.start('policy', 60, 7)[0]
        analyzer.timer_ctx.state = timer_state
        analyzer.stop()
        timer.assert_called_once_with(7, analyzer.timer_handler)
        timer.return_value.cancel.assert_called_once()
        timer.return_value.join.assert_called_once()
        assert analyzer.state == AnalyzerState.STOPPED
        assert analyzer.start('policy', 60, 7)[0]
        analyzer.stop()


def test_analyzer_cancel_joins_pending_callback():
    analyzer = Analyzer(Mock(), Mock(), Mock(), 'cephfs')
    assert analyzer.status()['state'] == AnalyzerState.STOPPED.name
    assert analyzer.start('policy', 100, 60)[0]
    timer = analyzer.timer_ctx.timer_task
    analyzer.stop()
    assert not timer.is_alive()
    analyzer.policy_mgr.update_workload_table.assert_not_called()


@pytest.mark.parametrize('owner', ['analyzer', 'mover', 'scheduler'])
@pytest.mark.parametrize('previous_owner', ['', 'same'])
@pytest.mark.parametrize('raises', [False, True])
def test_failed_start_restores_policy_owner(owner, previous_owner, raises):
    partition = object.__new__(FSMDSPartition)
    partition.lock = threading.Lock()
    policy_mgr = Mock()
    previous_owner = owner if previous_owner else ''
    policy_mgr.get_owner.return_value = previous_owner
    policy_mgr.is_active.return_value = True
    policy_mgr.list_subtree.return_value = (True, ['/subtree'])
    worker = Mock()
    worker.start.return_value = (False, 'cannot start')
    if raises:
        worker.start.side_effect = RuntimeError('cannot start')
    partition.context_map = {
        'cephfs': SimpleNamespace(policy_mgr=policy_mgr, **{owner: worker})}
    args = {'analyzer': ('60', '1'), 'mover': (), 'scheduler': (10, 10)}[owner]
    start = getattr(partition, owner + '_start')
    if raises:
        with pytest.raises(RuntimeError):
            start('cephfs', 'policy', *args)
    else:
        assert start('cephfs', 'policy', *args).retval < 0
    if previous_owner:
        policy_mgr.set_owner.assert_called_with('policy', previous_owner)
        policy_mgr.release_owner.assert_not_called()
    else:
        policy_mgr.release_owner.assert_called_once_with('policy')


@pytest.mark.parametrize('shutdown', [False, True])
def test_disabling_partitioner_joins_workers(mover, shutdown):
    partition = object.__new__(FSMDSPartition)
    partition.lock = threading.Lock()
    context = PartContext('cephfs')
    context.policy_mgr = mover.policy_mgr
    context.policy_mgr.show.return_value = (True, {})
    context.analyzer = Mock()
    context.mover = mover
    context.scheduler = Scheduler(context.analyzer, mover, context.policy_mgr)
    entered = threading.Event()

    def run_analyzer():
        entered.set()
        context.scheduler.stop_event.wait(2)
        return False

    context.scheduler.run_analyzer = run_analyzer
    partition.context_map = {'cephfs': context}
    assert context.scheduler.start('policy', 10, 10)[0]
    try:
        assert entered.wait(2)
        if shutdown:
            partition.shutdown()
        else:
            assert partition.partitioner_disable('cephfs').retval == 0
        assert not context.scheduler.thread.is_alive()
        assert not mover.wq.is_alive()
        assert not partition.context_map
        context.policy_mgr.stop.assert_called_once()
    finally:
        context.scheduler.stop()


def test_scheduler_accepts_ready_data_when_timer_has_finished():
    scheduler = make_scheduler()
    scheduler.policy_name = 'policy'
    scheduler.analyzer_period = 3600
    scheduler.policy_mgr.show.return_value = (True, {'workload_table_ready': 'True'})
    scheduler.analyzer.start.return_value = (True, 'started')
    scheduler.analyzer.status.return_value = {'timer_state': JobState.DONE.name}
    assert scheduler.run_analyzer() is True
    scheduler.analyzer.stop.assert_called_once()
