# -*- coding: utf-8 -*-
"""Shared daily cap for non-stock operational alert pushes."""
from __future__ import annotations

import json
import os
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Callable, Dict, Optional

SERVER_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATE_PATH = os.path.join(SERVER_DIR, 'data', 'service_alert_throttle.json')
DEFAULT_DAILY_LIMIT = 2
HARD_DAILY_LIMIT = 2

_LOG_LOCK = threading.Lock()
_LOCAL_LOCK = threading.Lock()
_LIMIT_LOG_DAY = ''
_LIMIT_LOG_SOURCES = set()


def _daily_limit() -> int:
    try:
        from services.support import app_settings
        if any(item['key'] == 'ops_service_alert_daily_limit'
               for item in app_settings.SPEC):
            configured = app_settings.value('ops_service_alert_daily_limit')
            if configured is not None:
                return max(0, min(HARD_DAILY_LIMIT, int(configured)))
    except Exception as e:
        print(f'[服务异常推送限频] 读取配置失败，使用默认上限 {DEFAULT_DAILY_LIMIT}/日: {e}')
    return DEFAULT_DAILY_LIMIT


@contextmanager
def _exclusive_file_lock():
    with _LOCAL_LOCK:
        lock_path = STATE_PATH + '.lock'
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        with open(lock_path, 'a+b') as lock_file:
            lock_file.seek(0, os.SEEK_END)
            if lock_file.tell() == 0:
                lock_file.write(b'\0')
                lock_file.flush()
            lock_file.seek(0)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
                unlock = lambda: msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                unlock = lambda: fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            try:
                yield
            finally:
                unlock()


def _read_state(day: str) -> Dict[str, object]:
    try:
        with open(STATE_PATH, 'r', encoding='utf-8') as f:
            state = json.load(f)
    except FileNotFoundError:
        return {'date': day, 'count': 0}
    if (not isinstance(state, dict) or not isinstance(state.get('date'), str)
            or not isinstance(state.get('count'), int) or state['count'] < 0):
        raise ValueError('状态文件格式无效')
    return state if state['date'] == day else {'date': day, 'count': 0}


def _write_state(state: Dict[str, object]) -> None:
    parent = os.path.dirname(STATE_PATH)
    os.makedirs(parent, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix='.service_alert_', suffix='.tmp', dir=parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as f:
            json.dump(state, f, ensure_ascii=False, separators=(',', ':'))
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, STATE_PATH)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def _log_limited(source: str, day: str, count: int, limit: int) -> None:
    global _LIMIT_LOG_DAY
    with _LOG_LOCK:
        if _LIMIT_LOG_DAY != day:
            _LIMIT_LOG_DAY = day
            _LIMIT_LOG_SOURCES.clear()
        if source in _LIMIT_LOG_SOURCES:
            return
        _LIMIT_LOG_SOURCES.add(source)
    print(f'[服务异常推送限频] {day} 已达每日上限 {limit} 次；'
          f'来源={source} 本次不推（已成功 {count} 次）')


def push_non_stock_alert(text: str, source: str,
                         sender: Optional[Callable[[str], bool]] = None) -> bool:
    """Send an operational alert if the shared daily quota has room.

    The quota is persisted across process restarts and shared by alert sources.
    Failed sends do not consume quota; the cross-process lock covers reservation
    and send so concurrent watchdogs cannot exceed the configured maximum.
    """
    day = datetime.now().date().isoformat()
    limit = _daily_limit()
    if limit == 0:
        _log_limited(source, day, 0, limit)
        return False
    try:
        with _exclusive_file_lock():
            state = _read_state(day)
            count = int(state['count'])
            if count >= limit:
                _log_limited(source, day, count, limit)
                return False
            reserved = {'date': day, 'count': count + 1}
            _write_state(reserved)
            try:
                if sender is None:
                    from services.support.push import push_wecom_webhook
                    send = push_wecom_webhook
                else:
                    send = sender
                pushed = bool(send(text))
            except Exception as e:
                print(f'[服务异常推送限频] 来源={source} 推送异常: {type(e).__name__}: {e}')
                pushed = False
            if not pushed:
                try:
                    _write_state({'date': day, 'count': count})
                except Exception as e:
                    print(f'[服务异常推送限频] 来源={source} 推送失败后回退额度失败，'
                          f'保留已预留额度: {type(e).__name__}: {e}')
            return pushed
    except Exception as e:
        print(f'[服务异常推送限频] 来源={source} 无法安全检查/保存每日额度，本次不推: '
              f'{type(e).__name__}: {e}')
        return False
