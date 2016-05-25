import logging
import datetime
import json
import socket

import pika
import sqlalchemy

from ckan.lib.base import config
from ckan.plugins import PluginImplementations
from ckan import model

from ckanext.harvest.model import (HarvestJob, HarvestObject,
                                   HarvestGatherError,
                                   HarvestObjectError)
from ckanext.harvest.interfaces import IHarvester

log = logging.getLogger(__name__)
assert not log.disabled

__all__ = ['get_gather_publisher', 'get_gather_consumer',
           'get_fetch_publisher', 'get_fetch_consumer',
           'get_harvester']

PORT = 5672
USERID = 'guest'
PASSWORD = 'guest'
HOSTNAME = 'localhost'
VIRTUAL_HOST = '/'
MQ_TYPE = 'amqp'
REDIS_PORT = 6379
REDIS_DB = 0

# settings for AMQP
EXCHANGE_TYPE = 'direct'
EXCHANGE_NAME = 'ckan.harvest'

def get_connection():
    backend = config.get('ckan.harvest.mq.type', MQ_TYPE)
    if backend in ('amqp', 'ampq'):  # "ampq" is for compat with old typo
        return get_connection_amqp()
    if backend == 'redis':
        return get_connection_redis()
    raise Exception('not a valid queue type %s' % backend)

def get_connection_amqp():
    try:
        port = int(config.get('ckan.harvest.mq.port', PORT))
    except ValueError:
        port = PORT
    userid = config.get('ckan.harvest.mq.user_id', USERID)
    password = config.get('ckan.harvest.mq.password', PASSWORD)
    hostname = config.get('ckan.harvest.mq.hostname', HOSTNAME)
    virtual_host = config.get('ckan.harvest.mq.virtual_host', VIRTUAL_HOST)

    credentials = pika.PlainCredentials(userid, password)
    parameters = pika.ConnectionParameters(host=hostname,
                                           port=port,
                                           virtual_host=virtual_host,
                                           credentials=credentials,
                                           frame_max=10000)
    log.debug("pika connection using %s" % parameters.__dict__)

    return pika.BlockingConnection(parameters)

def get_connection_redis():
    import redis
    return redis.StrictRedis(host=config.get('ckan.harvest.mq.hostname', HOSTNAME),
                          port=int(config.get('ckan.harvest.mq.port', REDIS_PORT)),
                          db=int(config.get('ckan.harvest.mq.redis_db', REDIS_DB)))


def get_gather_queue_name():
    return 'ckan.harvest.{0}.gather'.format(config.get('ckan.site_id',
                                                       'default'))


def get_fetch_queue_name():
    return 'ckan.harvest.{0}.fetch'.format(config.get('ckan.site_id',
                                                      'default'))


def get_gather_routing_key():
    return '{0}:harvest_job_id'.format(config.get('ckan.site_id',
                                                  'default'))


def get_fetch_routing_key():
    return '{0}:harvest_object_id'.format(config.get('ckan.site_id',
                                                     'default'))


def purge_queues():

    backend = config.get('ckan.harvest.mq.type', MQ_TYPE)
    connection = get_connection()
    if backend in ('amqp', 'ampq'):
        channel = connection.channel()
        channel.queue_purge(queue=get_gather_queue_name())
        log.info('AMQP queue purged: %s', get_gather_queue_name())
        channel.queue_purge(queue=get_fetch_queue_name())
        log.info('AMQP queue purged: %s', get_fetch_queue_name())
    elif backend == 'redis':
        get_gather_consumer().queue_purge(queue=get_gather_queue_name())
        get_fetch_consumer().queue_purge(queue=get_fetch_queue_name())
        log.info('Redis queues cleared')
    else:
        raise Exception('Unhandled backend: %s', backend)


def resubmit_jobs():
    '''
    Examines the fetch and gather queues for items that are supposedly
    currently being processed (persistence keys) that are suspiciously old.
    These are removed from the queues and placed back on them afresh, to ensure
    the fetch & gather consumers are triggered to process it.
    '''
    if config.get('ckan.harvest.mq.type') != 'redis':
        return
    redis = get_connection()

    # fetch queue
    harvest_object_pending = redis.keys(get_fetch_routing_key() + ':*')
    for key in harvest_object_pending:
        date_of_key = datetime.datetime.strptime(redis.get(key),
                                                 "%Y-%m-%d %H:%M:%S.%f")
        # 3 minutes max for fetch and import
        age = (datetime.datetime.utcnow() - date_of_key).seconds
        if age > 180:
            ho_id = key.split(':')[-1]
            log.info('Resubmitting to fetch queue harvest object id=%s '
                     'age=%ss',
                     ho_id, age)
            redis.rpush(get_fetch_routing_key(),
                        json.dumps({'harvest_object_id': ho_id}))
            redis.delete(key)

    # gather queue
    harvest_jobs_pending = redis.keys(get_gather_routing_key() + ':*')
    for key in harvest_jobs_pending:
        date_of_key = datetime.datetime.strptime(redis.get(key),
                                                 "%Y-%m-%d %H:%M:%S.%f")
        # 3 hours max for a gather
        age = (datetime.datetime.utcnow() - date_of_key).seconds
        if age > 7200:
            job_id = key.split(':')[-1]
            log.info('Resubmitting to gather queue harvest job id=%s age=%ss',
                     job_id, age)
            redis.rpush(get_gather_routing_key(),
                        json.dumps({'harvest_job_id': job_id}))
            redis.delete(key)


class Publisher(object):
    def __init__(self, connection, channel, exchange, routing_key):
        self.connection = connection
        self.channel = channel
        self.exchange = exchange
        self.routing_key = routing_key
    def send(self, body, **kw):
        return self.channel.basic_publish(self.exchange,
                                          self.routing_key,
                                          json.dumps(body),
                                          properties=pika.BasicProperties(
                                             delivery_mode = 2, # make message persistent
                                          ),
                                          **kw)
    def close(self):
        self.connection.close()

class RedisPublisher(object):
    def __init__(self, redis, routing_key):
        self.redis = redis ## not used
        self.routing_key = routing_key
    def send(self, body, **kw):
        value = json.dumps(body)
        # remove if already there
        if self.routing_key == get_gather_routing_key():
            self.redis.lrem(self.routing_key, 0, value)
        self.redis.rpush(self.routing_key, value)

    def close(self):
        return

def get_publisher(routing_key):
    connection = get_connection()
    backend = config.get('ckan.harvest.mq.type', MQ_TYPE)
    if backend in ('amqp', 'ampq'):
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, durable=True)
        return Publisher(connection,
                         channel,
                         EXCHANGE_NAME,
                         routing_key=routing_key)
    if backend == 'redis':
        return RedisPublisher(connection, routing_key)


class FakeMethod(object):
    ''' This is to act like the method returned by AMQP'''
    def __init__(self, message):
        self.delivery_tag = message


class RedisConsumer(object):
    def __init__(self, redis, routing_key):
        self.redis = redis
        # Routing keys are constructed with {site-id}:{message-key}, eg:
        # default:harvest_job_id or default:harvest_object_id
        self.routing_key = routing_key
        # Message keys are harvest_job_id for the gather consumer and
        # harvest_object_id for the fetch consumer
        self.message_key = routing_key.split(':')[-1]

    def consume(self, queue):
        while True:
            key, body = self.redis.blpop(self.routing_key)
            # Create a 'persistence key' to show we are currently
            # processing a particular item in the main queues,
            # which will get deleted again on ack. If an error
            # causes it to be left around, then resubmit kicks in.
            self.redis.set(self._persistence_key(body),
                           str(datetime.datetime.utcnow()))
            yield (FakeMethod(body), self, body)

    def _persistence_key(self, message):
        # Persistence keys are constructed with
        # {site-id}:{message-key}:{object-id}, eg:
        # default:harvest_job_id:804f114a-8f68-4e7c-b124-3eb00f66202e
        message = json.loads(message)
        return self.routing_key + ':' + message[self.message_key]

    def basic_ack(self, message):
        self.redis.delete(self._persistence_key(message))

    def queue_purge(self, queue):
        self.redis.flushdb()

    def basic_get(self, queue):
        body = self.redis.lpop(self.routing_key)
        return (FakeMethod(body), self, body)


def get_consumer(queue_name, routing_key):

    connection = get_connection()
    backend = config.get('ckan.harvest.mq.type', MQ_TYPE)

    if backend in ('amqp', 'ampq'):
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, durable=True)
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(queue=queue_name, exchange=EXCHANGE_NAME, routing_key=routing_key)
        return channel
    if backend == 'redis':
        return RedisConsumer(connection, routing_key)


def gather_callback(channel, method, header, body):
    try:
        id = json.loads(body)['harvest_job_id']
        log.debug('Received harvest job id: %s', id)
    except KeyError:
        log.error('No harvest job id received')
        channel.basic_ack(method.delivery_tag)
        return False

    # Get rid of any old session state that may still be around. This is
    # a simple alternative to creating a new session for this callback.
    model.Session.expire_all()

    # Get a publisher for the fetch queue
    publisher = get_fetch_publisher()

    try:
        job = HarvestJob.get(id)
    except sqlalchemy.exc.OperationalError, e:
        # Occasionally we see: sqlalchemy.exc.OperationalError
        # "SSL connection has been closed unexpectedly"
        log.exception(e)
        log.error('Connection Error during gather of job %s: %r %r',
                  id, e, e.args)
        # By not sending the ack, it will be retried later.
        # Try to clear the issue with a remove
        model.Session.remove()
        return

    try:
        if not job:
            log.error('Harvest job does not exist: %s', id)
            return

        if job.status != 'Running':
            if job.status == 'Aborted':
                log.info('Harvest job has been aborted: %s', id)
            else:
                log.error('Harvest job invalid - '
                          'status should be "Running" but was %s '
                          'id=%s created=%s',
                          job.status, id, str(job.created))
            return

        # Send the harvest job to the plugins that implement
        # the Harvester interface, only if the source type
        # matches
        harvester = get_harvester(job.source.type)
        if harvester:
            try:
                harvest_object_ids = gather_stage(harvester, job)
            except (Exception, KeyboardInterrupt):
                raise

            if not isinstance(harvest_object_ids, list):
                log.error('Gather stage failed')
                return False  # not sure the False does anything

            if len(harvest_object_ids) == 0:
                log.info('No harvest objects to fetch')
                return False  # not sure the False does anything

            log.debug('Received from plugin gather_stage, to send to fetch queue: {0} objects (first ho id: {1} last: {2})'.format(
                      len(harvest_object_ids), harvest_object_ids[:1], harvest_object_ids[-1:]))
            for id in harvest_object_ids:
                # Send the id to the fetch queue
                publisher.send({'harvest_object_id': id})
            log.debug('Sent {0} objects to the fetch queue'.format(
                len(harvest_object_ids)))

        else:
            # This can occur if you:
            # * remove a harvester and it still has sources that are then
            #   refreshed
            # * add a new harvester and restart CKAN but not the gather
            #   queue.
            msg = 'System error - no harvester could be found for source'\
                  ' type %s' % job.source.type
            HarvestGatherError.create(message=msg, job=job)
            log.error(msg)
            job.status = 'Aborted'
            job.save()

    finally:
        model.Session.remove()
        publisher.close()
        channel.basic_ack(method.delivery_tag)


def get_harvester(harvest_source_type):
    for harvester in PluginImplementations(IHarvester):
        if harvester.info()['name'] == harvest_source_type:
            return harvester


def gather_stage(harvester, job):
    '''Calls the harvester's gather_stage, returning harvest object ids, with
    some error handling.

    This is split off from gather_callback so that tests can call it without
    dealing with queue stuff.
    '''
    # Get a list of harvest object ids from the plugin
    job.gather_started = datetime.datetime.utcnow()
    job.save()
    try:
        harvest_object_ids = harvester.gather_stage(job)
    except (Exception, KeyboardInterrupt), e:
        # Assume it is a serious error and the harvest stops
        # now, so tidy up.
        log.exception(e)
        log.error('Gather exception: %r', e)
        HarvestGatherError.create(
            message='System error during harvest: %s' % e, job=job)
        job.status = 'Aborted'
        # Delete any harvest objects else they'd suggest the
        # job is in limbo
        harvest_objects = model.Session.query(HarvestObject). \
            filter_by(harvest_job_id=job.id)
        for harvest_object in harvest_objects.all():
            model.Session.delete(harvest_object)
        model.Session.commit()
        raise
    finally:
        job.gather_finished = datetime.datetime.utcnow()
        job.save()
    harvest_object_ids = harvest_object_ids or []
    log.debug('Received from plugin gather_stage: {0} objects '
              '(first ho id: {1} last: {2})'.format(
        len(harvest_object_ids), harvest_object_ids[:1],
        harvest_object_ids[-1:]))

    # Delete any stray harvest_objects not returned by
    # gather_stage() - they'd not be dealt with so would
    # suggest the job is in limbo
    saved_harvest_object_ids = [
        ho.id for ho in
        model.Session.query(HarvestObject)
        .filter_by(harvest_job_id=job.id).all()]
    orphaned_harvest_objects_ids = \
        set(saved_harvest_object_ids) - \
        set(harvest_object_ids or [])
    if orphaned_harvest_objects_ids:
        log.warning('Orphaned objects deleted (%d): %s',
                    len(orphaned_harvest_objects_ids),
                    orphaned_harvest_objects_ids)
        for obj_id in orphaned_harvest_objects_ids:
            model.Session.delete(HarvestObject.get(obj_id))
        model.Session.commit()
    return harvest_object_ids


def fetch_callback(channel, method, header, body):
    try:
        id = json.loads(body)['harvest_object_id']
        log.info('Received harvest object id: %s' % id)
    except KeyError:
        log.error('No harvest object id received')
        channel.basic_ack(method.delivery_tag)
        return False

    # Get rid of any old session state that may still be around. This is
    # a simple alternative to creating a new session for this callback.
    model.Session.expire_all()

    try:
        obj = HarvestObject.get(id)
    except sqlalchemy.exc.OperationalError, e:
        # Occasionally we see: sqlalchemy.exc.OperationalError
        # "SSL connection has been closed unexpectedly"
        log.exception(e)
        log.error('Connection Error during gather of harvest object %s: %r %r',
                  id, e, e.args)
        # By not sending the ack, it will be retried later.
        # Try to clear the issue with a remove.
        model.Session.remove()
        return

    try:
        if not obj:
            log.error('Harvest object does not exist: %s' % id)
            return

        obj.retry_times += 1
        obj.save()

        if obj.retry_times >= 5:
            obj.state = "ERROR"
            obj.save()
            log.error('Too many consecutive retries for object {0}'.format(obj.id))
            channel.basic_ack(method.delivery_tag)
            return False


        # Send the harvest object to the plugins that implement
        # the Harvester interface, only if the source type
        # matches
        for harvester in PluginImplementations(IHarvester):
            if harvester.info()['name'] == obj.source.type:
                fetch_and_import_stages(harvester, obj)

    finally:
        model.Session.remove()
        channel.basic_ack(method.delivery_tag)


def fetch_and_import_stages(harvester, obj):

    # See if the plugin can fetch the harvest object
    obj.fetch_started = datetime.datetime.utcnow()
    obj.state = "FETCH"
    obj.save()
    try:
        success_fetch = harvester.fetch_stage(obj)
    except Exception, e:
        msg = 'System error (%s)' % e
        HarvestObjectError.create(
            message=msg, object=obj, stage='Fetch', line=None)
        log.error('Fetch exception %s obj=%s guid=%s source=%s',
                  e, obj.id, obj.guid, obj.source.id)
        log.exception(e)
        success_fetch = False
    finally:
        obj.fetch_finished = datetime.datetime.utcnow()
        obj.save()
    #TODO: retry times?
    if success_fetch is True:
        # If no errors were found, call the import method
        obj.import_started = datetime.datetime.utcnow()
        obj.state = "IMPORT"
        obj.save()
        try:
            success_import = harvester.import_stage(obj)
        except Exception, e:
            msg = 'System error (%s)' % e
            HarvestObjectError.create(
                message=msg, object=obj, stage='Import', line=None)
            log.error('Import exception: %s obj=%s guid=%s source=%s',
                        e, obj.id, obj.guid, obj.source.id)
            log.exception(e)
            success_import = False
        finally:
            obj.import_finished = datetime.datetime.utcnow()
            obj.save()
        if success_import:
            obj.state = "COMPLETE"
            if success_import is 'unchanged':
                obj.report_status = 'not modified'
                obj.save()
                return
        else:
            obj.state = "ERROR"
        obj.save()
    elif success_fetch == 'unchanged':
        obj.report_status = 'not modified'
        obj.state = "COMPLETE"
        obj.save()
    else:
        obj.state = "ERROR"
        obj.save()
    if obj.report_status:
        return
    if obj.state == 'ERROR':
        obj.report_status = 'errored'
    elif obj.get_extra('status') == 'deleted':
        obj.report_status = 'deleted'
    elif obj.current == False:
        # decided not to continue with the import after all
        obj.report_status = 'not modified'
    elif len(model.Session.query(HarvestObject)
        .filter_by(package_id=obj.package_id)
        .limit(2)
        .all()) == 2:
        obj.report_status = 'updated'
    else:
        obj.report_status = 'added'
    obj.save()

def get_gather_consumer():
    gather_routing_key = get_gather_routing_key()
    consumer = get_consumer(get_gather_queue_name(), gather_routing_key)
    log.debug('Gather queue consumer registered')
    return consumer


def get_fetch_consumer():
    fetch_routing_key = get_fetch_routing_key()
    consumer = get_consumer(get_fetch_queue_name(), fetch_routing_key)
    log.debug('Fetch queue consumer registered')
    return consumer


def get_gather_publisher():
    gather_routing_key = get_gather_routing_key()
    return get_publisher(gather_routing_key)


def get_fetch_publisher():
    fetch_routing_key = get_fetch_routing_key()
    return get_publisher(fetch_routing_key)
