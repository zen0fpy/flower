import os
import time
import shelve
import logging
import threading
import collections

from functools import partial

from tornado.ioloop import IOLoop
from tornado.ioloop import PeriodicCallback

from celery.events import EventReceiver
from celery.events.state import State
from tornado.options import options

from . import api

from collections import Counter

from prometheus_client import Counter as PrometheusCounter, Histogram, Gauge

logger = logging.getLogger(__name__)

prometheus_metrics = None


def get_prometheus_metrics():
    global prometheus_metrics
    if prometheus_metrics is None:
        prometheus_metrics = PrometheusMetrics()

    return prometheus_metrics


class PrometheusMetrics(object):
    # 监控指标, 可以保存到promethus 或者influxdb
    def __init__(self):
        self.events = PrometheusCounter('flower_events_total', "Number of events", ['worker', 'type', 'task'])

        self.runtime = Histogram(
            'flower_task_runtime_seconds',
            "Task runtime",
            ['worker', 'task'],
            buckets=options.task_runtime_metric_buckets
        )
        self.prefetch_time = Gauge(
            'flower_task_prefetch_time_seconds',
            "The time the task spent waiting at the celery worker to be executed.",
            ['worker', 'task']
        )
        self.number_of_prefetched_tasks = Gauge(
            'flower_worker_prefetched_tasks',
            'Number of tasks of given type prefetched at a worker',
            ['worker', 'task']
        )
        self.worker_online = Gauge('flower_worker_online', "Worker online status", ['worker'])
        self.worker_number_of_currently_executing_tasks = Gauge(
            'flower_worker_number_of_currently_executing_tasks',
            "Number of tasks currently executing at a worker",
            ['worker']
        )


class EventsState(State):
    """封装celery State基类"""
    # EventsState object is created and accessed only from ioloop thread

    def __init__(self, *args, **kwargs):
        super(EventsState, self).__init__(*args, **kwargs)
        # 设置counter默认值
        self.counter = collections.defaultdict(Counter)
        self.metrics = get_prometheus_metrics()

    def event(self, event):
        """保存数据"""
        # Save the event
        super(EventsState, self).event(event)

        # worker名字
        worker_name = event['hostname']
        # 事件类型: started， failed, received等
        event_type = event['type']

        self.counter[worker_name][event_type] += 1

        # 任务事件
        if event_type.startswith('task-'):
            # 任务id
            task_id = event['uuid']
            # 任务信息
            task = self.tasks.get(task_id)
            # 任务名
            task_name = event.get('name', '')
            if not task_name and task_id in self.tasks:
                task_name = task.name or ''
            self.metrics.events.labels(worker_name, event_type, task_name).inc()

            runtime = event.get('runtime', 0)
            if runtime:
                self.metrics.runtime.labels(worker_name, task_name).observe(runtime)

            task_started = task.started
            task_received = task.received

            if event_type == 'task-received' and not task.eta and task_received:
                self.metrics.number_of_prefetched_tasks.labels(worker_name, task_name).inc()

            if event_type == 'task-started' and not task.eta and task_started and task_received:
                self.metrics.prefetch_time.labels(worker_name, task_name).set(task_started - task_received)
                self.metrics.number_of_prefetched_tasks.labels(worker_name, task_name).dec()

            if event_type in ['task-succeeded', 'task-failed'] and not task.eta and task_started and task_received:
                self.metrics.prefetch_time.labels(worker_name, task_name).set(0)

        if event_type == 'worker-online':
            self.metrics.worker_online.labels(worker_name).set(1)

        if event_type == 'worker-heartbeat':
            self.metrics.worker_online.labels(worker_name).set(1)

            num_executing_tasks = event.get('active')
            if num_executing_tasks is not None:
                self.metrics.worker_number_of_currently_executing_tasks.labels(worker_name).set(num_executing_tasks)

        if event_type == 'worker-offline':
            self.metrics.worker_online.labels(worker_name).set(0)

        # 通过websocket api发送任务事件到前端页面
        # Send event to api subscribers (via websockets)
        classname = api.events.getClassName(event_type)
        cls = getattr(api.events, classname, None)
        if cls:
            cls.send_message(event)



class Events(threading.Thread):
    # 封装线程基类， 用线程方式启动
    # 在后台执行来监听事件
    events_enable_interval = 5000

    def __init__(self, capp, db=None, persistent=False,
                 enable_events=True, io_loop=None, state_save_interval=0,
                 **kwargs):
        threading.Thread.__init__(self)
        self.daemon = True

        self.io_loop = io_loop or IOLoop.instance()
        self.capp = capp

        self.db = db
        # 事件状态是否持久化
        self.persistent = persistent
        # 激活事件
        self.enable_events = enable_events
        self.state = None
        # 保存定时器
        self.state_save_timer = None

        if self.persistent:
            logger.debug("Loading state from '%s'...", self.db)
            state = shelve.open(self.db)
            if state:
                self.state = state['events']
            state.close()

            # 多久保存一次状态信息
            # 定时运行回调函数
            if state_save_interval:
                self.state_save_timer = PeriodicCallback(self.save_state,
                                                         state_save_interval)

        # 初始化状态
        if not self.state:
            self.state = EventsState(**kwargs)

        # 定时激活事件
        self.timer = PeriodicCallback(self.on_enable_events,
                                      self.events_enable_interval)

    def start(self):
        threading.Thread.start(self)
        if self.enable_events:
            logger.debug("Starting enable events timer...")
            self.timer.start()

        if self.state_save_timer:
            logger.debug("Starting state save timer...")
            self.state_save_timer.start()

    def stop(self):
        if self.enable_events:
            logger.debug("Stopping enable events timer...")
            self.timer.stop()

        if self.state_save_timer:
            logger.debug("Stopping state save timer...")
            self.state_save_timer.stop()

        if self.persistent:
            self.save_state()

    def run(self):
        """
            run函数在线程启动，被start函数调用
        """
        try_interval = 1
        while True:
            try:
                try_interval *= 2

                # 捕获celery事件
                # handlers里面是事件回调函数
                with self.capp.connection() as conn:
                    recv = EventReceiver(conn,
                                         handlers={"*": self.on_event},
                                         app=self.capp)
                    # 重置 重试间隔
                    try_interval = 1
                    logger.debug("Capturing events...")
                    recv.capture(limit=None, timeout=None, wakeup=True)
            except (KeyboardInterrupt, SystemExit):
                try:
                    import _thread as thread
                except ImportError:
                    import thread
                # 会在主线程中抛出一个键盘中断异常
                thread.interrupt_main()
            except Exception as e:
                logger.error("Failed to capture events: '%s', "
                             "trying again in %s seconds.",
                             e, try_interval)
                logger.debug(e, exc_info=True)
                time.sleep(try_interval)

    def save_state(self):
        logger.debug("Saving state to '%s'...", self.db)
        state = shelve.open(self.db, flag='n')
        state['events'] = self.state
        state.close()

    def on_enable_events(self):
        # Periodically enable events for workers
        # launched after flower
        # 执行worker激活事件
        self.io_loop.run_in_executor(None, self.capp.control.enable_events)

    def on_event(self, event):
        # Call EventsState.event in ioloop thread to avoid synchronization
        # 监听事件，进行处理
        self.io_loop.add_callback(partial(self.state.event, event))
