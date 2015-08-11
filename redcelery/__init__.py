import contextlib

import celery
import celery.backends.redis


VERSION = celery.version_info_t(3, 1, 18, '', '')

if celery.VERSION != VERSION:
    raise Exception('redcelery 3.1.18 patches celery version 3.1.18 only')


_RedisBackend_params_from_url = celery.backends.redis.RedisBackend._params_from_url
_RedisBackend__set = celery.backends.redis.RedisBackend._set
_RedisBackend__new_chord_return = celery.backends.redis.RedisBackend._new_chord_return


def _params_from_url_fix_timeouts(self, url, defaults):
    connparams = _RedisBackend_params_from_url(self, url, defaults)
    for key in ['socket_timeout', 'socket_connect_timeout']:
        if key in connparams:
            connparams[key] = float(connparams[key])
    return connparams


def _set(self, key, value):
    with self.client.pipeline() as pipe:
        if self.expires:
            pipe.setex(key, value, self.expires)
        else:
            pipe.set(key, value)
        pipe.publish(key, value)
        pipe.execute()


def _new_chord_return(self, task, state, result, propagate=None,
                      PROPAGATE_STATES=celery.backends.redis.states.PROPAGATE_STATES):
    app = self.app
    if propagate is None:
        propagate = self.app.conf.CELERY_CHORD_PROPAGATES
    request = task.request
    tid, gid = request.id, request.group
    if not gid or not tid:
        return

    client = self.client
    jkey = self.get_key_for_group(gid, '.j')
    result = self.encode_result(result, state)
    with client.pipeline() as pipe:
        _, readycount, _ = (
            pipe
            .rpush(jkey, self.encode([1, tid, state, result]))
            .llen(jkey)
            .expire(jkey, 86400)
            .execute())

    try:
        callback = celery.backends.redis.maybe_signature(request.chord, app=app)
        total = callback['chord_size']
        if readycount == total:
            decode, unpack = self.decode, self._unpack_chord_result
            with client.pipeline() as pipe:
                resl, _ = (
                    pipe
                    .lrange(jkey, 0, total)
                    .delete(jkey)
                    .execute())
            try:
                callback.delay([unpack(tup, decode) for tup in resl])
            except Exception as exc:
                celery.backends.redis.error('Chord callback for %r raised: %r',
                                            request.group, exc, exc_info=1)
                app._tasks[callback.task].backend.fail_from_current_stack(
                    callback.id,
                    exc=celery.backends.redis.ChordError('Callback error: {0!r}'.format(exc)),
                )
    except celery.backends.redis.ChordError as exc:
        celery.backends.redis.error('Chord %r raised: %r', request.group, exc, exc_info=1)
        app._tasks[callback.task].backend.fail_from_current_stack(
            callback.id, exc=exc,
        )
    except Exception as exc:
        celery.backends.redis.error('Chord %r raised: %r', request.group, exc, exc_info=1)
        app._tasks[callback.task].backend.fail_from_current_stack(
            callback.id, exc=celery.backends.redis.ChordError('Join error: {0!r}'.format(exc)),
        )


def patch():
    celery.backends.redis.RedisBackend._params_from_url = _params_from_url_fix_timeouts
    celery.backends.redis.RedisBackend._set = _set
    celery.backends.redis.RedisBackend._new_chord_return = _new_chord_return


def unpatch():
    celery.backends.redis.RedisBackend._params_from_url = _RedisBackend_params_from_url
    celery.backends.redis.RedisBackend._set = _RedisBackend__set
    celery.backends.redis.RedisBackend._new_chord_return = _RedisBackend__new_chord_return


@contextlib.contextmanager
def patch_context():
    patch()
    try:
        yield
    finally:
        unpatch()
