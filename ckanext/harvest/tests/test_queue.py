try:
    from ckan.tests.helpers import reset_db
except ImportError:
    from ckan.new_tests.helpers import reset_db

import ckanext.harvest.model as harvest_model
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from ckanext.harvest.interfaces import IHarvester
import ckanext.harvest.queue as queue
from ckanext.harvest.logic import HarvestJobExists
from ckan.plugins.core import SingletonPlugin, implements
from ckan.plugins import toolkit as tk
from ckan.new_tests import factories
import json
import ckan.logic as logic
from ckan import model
from nose.tools import assert_equal, assert_raises

get_action = tk.get_action

import logging
log = logging.getLogger(__name__)

# Get message off a carrot queue
def pop(consumer):
    message = consumer.fetch()
    assert message
    return message.payload, message


class MockHarvester(SingletonPlugin):
    implements(IHarvester)
    def info(self):
        return {'name': 'test', 'title': 'test', 'description': 'test'}

    def gather_stage(self, harvest_job):

        if harvest_job.source.url.startswith('basic_test'):
            obj = HarvestObject(guid = 'test1', job = harvest_job)
            obj.extras.append(HarvestObjectExtra(key='key', value='value'))
            obj2 = HarvestObject(guid = 'test2', job = harvest_job)
            obj3 = HarvestObject(guid = 'test_to_delete', job = harvest_job)
            obj.add()
            obj2.add()
            obj3.save() # this will commit both
            return [obj.id, obj2.id, obj3.id]

        return []

    def fetch_stage(self, harvest_object):
        assert_equal(harvest_object.state, "FETCH")
        assert harvest_object.fetch_started != None
        harvest_object.content = json.dumps({'name': harvest_object.guid})
        harvest_object.save()
        return True

    def import_stage(self, harvest_object):
        assert_equal(harvest_object.state, "IMPORT")
        assert harvest_object.fetch_finished != None
        assert harvest_object.import_started != None

        user = logic.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )['name']

        package = json.loads(harvest_object.content)
        name = package['name']

        package_object = model.Package.get(name)
        if package_object:
            logic_function = 'package_update'
        else:
            logic_function = 'package_create'

        package_dict = logic.get_action(logic_function)(
            {'model': model, 'session': model.Session,
             'user': user, 'api_version': 3, 'ignore_auth': True},
            json.loads(harvest_object.content)
        )

        # set previous objects to not current
        previous_object = model.Session.query(HarvestObject) \
                          .filter(HarvestObject.guid==harvest_object.guid) \
                          .filter(HarvestObject.current==True) \
                          .first()
        if previous_object:
            previous_object.current = False
            previous_object.save()

        # delete test_to_delete package on second run
        harvest_object.package_id = package_dict['id']
        harvest_object.current = True
        if package_dict['name'] == 'test_to_delete' and package_object:
            harvest_object.current = False
            package_object.state = 'deleted'
            package_object.save()

        harvest_object.save()
        return True


class TestHarvestQueue(object):
    '''Tests queue.py by calling its methods directly and testing what it does
    in detail.'''
    @classmethod
    def setup_class(cls):
        reset_db()
        harvest_model.setup()

    def test_01_basic_harvester(self):

        ### make sure queues/exchanges are created first and are empty
        consumer = queue.get_consumer('ckan.harvest.gather','harvest_job_id')
        consumer_fetch = queue.get_consumer('ckan.harvest.fetch','harvest_object_id')
        # DGU Hack - equivalent for carrot
        connection = queue.get_carrot_connection()
        connection.get_channel().queue_purge('ckan.harvest.gather')
        connection.get_channel().queue_purge('ckan.harvest.fetch')
        #consumer.queue_purge(queue='ckan.harvest.gather')
        #consumer_fetch.queue_purge(queue='ckan.harvest.fetch')

        factories.Organization(name='testorg')

        user = logic.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )['name']

        context = {'model': model, 'session': model.Session,
                   'user': user, 'api_version': 3, 'ignore_auth': True}
        context_fresh = context.copy()

        source_dict = {
            'title': 'Test Source',
            'name': 'test-source',
            'url': 'basic_test',
            # DGU HACK
            'type': 'test',
            #'source_type': 'test',
            'publisher_id': 'testorg',
        }

        harvest_source = logic.get_action('harvest_source_create')(
            context,
            source_dict
        )

        #assert harvest_source['source_type'] == 'test', harvest_source
        assert harvest_source['type'] == 'test', harvest_source
        assert harvest_source['url'] == 'basic_test', harvest_source


        harvest_job = logic.get_action('harvest_job_create')(
            context,
            {'source_id':harvest_source['id'], 'run': True}
        )

        job_id = harvest_job['id']

        assert harvest_job['source_id'] == harvest_source['id'], harvest_job

        assert harvest_job['status'] == u'Running'

        assert logic.get_action('harvest_job_show')(
            context,
            {'id': job_id}
        )['status'] == u'Running'

        ## pop on item off the queue and run the callback
        #DGU HACKS
        #reply = consumer.basic_get(queue='ckan.harvest.gather')
        queue.gather_callback(*pop(consumer))

        all_objects = model.Session.query(HarvestObject).all()

        assert len(all_objects) == 3
        assert all_objects[0].state == 'WAITING'
        assert all_objects[1].state == 'WAITING'
        assert all_objects[2].state == 'WAITING'


        assert len(model.Session.query(HarvestObject).all()) == 3
        assert len(model.Session.query(HarvestObjectExtra).all()) == 1

        ## do three times as three harvest objects
        queue.fetch_callback(*pop(consumer_fetch))
        queue.fetch_callback(*pop(consumer_fetch))
        queue.fetch_callback(*pop(consumer_fetch))

        count = model.Session.query(model.Package) \
                .filter(model.Package.type=='dataset') \
                .count()
        assert count == 3
        all_objects = model.Session.query(HarvestObject).filter_by(current=True).all()

        assert_equal(len(all_objects), 3)
        assert_equal(all_objects[0].state, 'COMPLETE')
        assert_equal(all_objects[0].report_status, 'added')
        assert_equal(all_objects[1].state, 'COMPLETE')
        assert_equal(all_objects[1].report_status, 'added')
        assert_equal(all_objects[2].state, 'COMPLETE')
        assert_equal(all_objects[2].report_status, 'added')

        ## fire run again to check if job is set to Finished
        logic.get_action('harvest_jobs_run')(
            context,
            {'source_id':harvest_source['id']}
        )

        harvest_job = logic.get_action('harvest_job_show')(
            context_fresh.copy(),
            {'id': job_id}
        )

        assert_equal(harvest_job['status'], u'Finished')
        assert_equal(harvest_job['stats'], {'added': 3, 'updated': 0, 'not modified': 0, 'errored': 0, 'deleted': 0})

        context['include_status'] = True  # DGU only
        context['include_job_status'] = True  # DGU only
        harvest_source_dict = logic.get_action('harvest_source_show')(
            context,
            {'id': harvest_source['id']}
        )

        assert_equal(harvest_source_dict['status']['last_job']['stats'], {'added': 3, 'updated': 0, 'not modified': 0, 'errored': 0, 'deleted': 0})
        assert_equal(harvest_source_dict['status']['total_datasets'], 3)
        assert_equal(harvest_source_dict['status']['job_count'], 1)

        ########### Second run ########################
        harvest_job = logic.get_action('harvest_job_create')(
            context,
            {'source_id':harvest_source['id'], 'run': True}
        )

        job_id = harvest_job['id']
        assert logic.get_action('harvest_job_show')(
            context,
            {'id': job_id}
        )['status'] == u'Running'

        ## pop on item off the queue and run the callback
        queue.gather_callback(*pop(consumer))

        all_objects = model.Session.query(HarvestObject).all()

        assert len(all_objects) == 6

        queue.fetch_callback(*pop(consumer_fetch))
        queue.fetch_callback(*pop(consumer_fetch))
        queue.fetch_callback(*pop(consumer_fetch))

        count = model.Session.query(model.Package) \
                .filter(model.Package.type=='dataset') \
                .count()
        assert count == 3

        all_objects = model.Session.query(HarvestObject).filter_by(report_status='added').all()
        assert len(all_objects) == 3, len(all_objects)

        all_objects = model.Session.query(HarvestObject).filter_by(report_status='updated').all()
        assert len(all_objects) == 2, len(all_objects)

        all_objects = model.Session.query(HarvestObject).filter_by(report_status='not modified').all()
        assert len(all_objects) == 1, len(all_objects)

        # run to make sure job is marked as finshed
        logic.get_action('harvest_jobs_run')(
            context,
            {'source_id':harvest_source['id']}
        )

        context_ = context.copy()
        context_['return_stats'] = True
        harvest_job = logic.get_action('harvest_job_show')(
            context_,
            {'id': job_id}
        )
        assert_equal(harvest_job['stats'], {'added': 0, 'updated': 2, 'not modified': 1, 'errored': 0, 'deleted': 0})

        context['detailed'] = True
        context['include_status'] = True  # DGU only
        context['include_job_status'] = True  # DGU only
        harvest_source_dict = logic.get_action('harvest_source_show')(
            context,
            {'id': harvest_source['id']}
        )

        assert_equal(harvest_job['stats'], {'added': 0, 'updated': 2, 'not modified': 1, 'errored': 0, 'deleted': 0})
        assert_equal(harvest_source_dict['status']['total_datasets'], 2)
        assert_equal(harvest_source_dict['status']['job_count'], 2)


class GatherException(Exception):
    pass
class FetchException(Exception):
    pass


class BadHarvester(SingletonPlugin):
    '''This harvester can be configured to behave badly in lots of ways, to
    help test queue.py'''
    implements(IHarvester)
    def info(self):
        return {'name': 'bad', 'title': 'bad', 'description': 'test harvester'}

    def gather_stage(self, harvest_job):
        if harvest_job.source.url == 'gather_excepts':
            raise GatherException()

        if harvest_job.source.url == 'gather_excepts_after_creating_objects':
            obj = HarvestObject(guid='test1', job=harvest_job)
            obj.add()
            obj.save()
            raise GatherException()

        obj = HarvestObject(guid='test1', job=harvest_job)
        obj.add()
        obj.save()
        return [obj.id]

    def fetch_stage(self, harvest_object):
        if harvest_object.job.source.url == 'fetch_excepts':
            raise FetchException()


class TestHarvestQueueBlackBox(object):
    '''Tests queue.py as a black box, calling it using "job-run" (which mimics
    celery's calls, but all in-process) and checking the state of the job at
    the end.
    '''

    @classmethod
    def setup_class(cls):
        cls._empty_the_queues()

    def setup(self):
        harvest_model.setup()
        self.site_user = logic.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )['name']

    @classmethod
    def teardown_class(cls):
        model.repo.rebuild_db()

    def teardown(self):
        self._empty_the_queues()
        model.repo.rebuild_db()

    @classmethod
    def _empty_the_queues(cls):
        ### make sure queues/exchanges are created first and are empty
        gather_consumer = queue.get_consumer('ckan.harvest.gather','harvest_job_id')
        fetch_consumer = queue.get_consumer('ckan.harvest.fetch','harvest_object_id')
        # DGU Hack - equivalent for carrot
        connection = queue.get_carrot_connection()
        connection.get_channel().queue_purge('ckan.harvest.gather')
        connection.get_channel().queue_purge('ckan.harvest.fetch')
        #gather_consumer.queue_purge(queue='ckan.harvest.gather')
        #fetch_consumer.queue_purge(queue='ckan.harvest.fetch')

    def _assert_queues_are_empty(self):
        gather_consumer = queue.get_consumer('ckan.harvest.gather','harvest_job_id')
        fetch_consumer = queue.get_consumer('ckan.harvest.fetch','harvest_object_id')
        for queue_name, consumer in (('gather', gather_consumer),
                                     ('fetch', fetch_consumer)):
            msg = consumer.fetch()
            assert not msg, 'Did not expect message on %s queue: %s' % \
                (queue_name, msg)

    def _create_source(self, url, source_type='bad'):
        factories.Organization(name='testorg')
        context = {'model': model, 'session': model.Session,
                   'user': self.site_user, 'api_version': 3, 'ignore_auth': True}
        source_dict = {
            'title': 'Test Source',
            'name': 'test-source',
            'url': url,
            # DGU HACK
            'type': source_type,
            #'source_type': source_type,
            'publisher_id': 'testorg',
        }
        harvest_source = get_action('harvest_source_create')(
            context,
            source_dict
        )
        return harvest_source

    def _job_run(self, source_id):
        logging.getLogger('amqplib').setLevel(logging.INFO)

        source_id = unicode(source_id)

        # ensure the queues are empty - needed for this command to run ok
        self._assert_queues_are_empty()

        # get the source id first (source_id may be a source.name)
        context = {'model': model, 'user': self.site_user,
                   'session': model.Session}
        source = get_action('harvest_source_show')(context,
                                                   {'id': source_id})
        # create harvest job
        try:
            job = get_action('harvest_job_create')(
                context,
                {'source_id': source['id'], 'run': True})
        except HarvestJobExists:
            log.debug('Job exists - cannot create it')
            raise

        # gather
        log.info('Gather')
        gather_consumer = queue.get_consumer('ckan.harvest.gather', 'harvest_job_id')
        message = gather_consumer.fetch()
        if not message:
            log.error('Could not get gather message - probably because the gather process is running elsewhere.')
            assert 0, 'No gather message'
        queue.gather_callback({'harvest_job_id': job['id']}, message)

        # fetch
        logging.getLogger('ckan.cli').info('Fetch')
        fetch_consumer = queue.get_consumer('ckan.harvest.fetch', 'harvest_object_id')
        while True:
            message = fetch_consumer.fetch()
            if not message:
                break
            queue.fetch_callback(message.payload, message)

        # run - mark the job as finished
        get_action('harvest_jobs_run')(context, {'source_id': source['id']})

        # get details of the job and source
        return (self._harvest_job_show(job['id']),
                self._harvest_source_show(source['id']))

    def _harvest_job_show(self, job_id=None):
        context = {'model': model, 'user': self.site_user,
                   'session': model.Session}
        if job_id is None:
            jobs = get_action('harvest_job_list')(context, {})
            job_id = jobs[0]['id']  # latest one
        context['return_stats'] = True
        harvest_job = logic.get_action('harvest_job_show')(
            context,
            {'id': job_id}
        )
        return harvest_job

    def _harvest_source_show(self, source_id=None):
        context = {'model': model, 'user': self.site_user,
                   'session': model.Session}
        if not source_id:
            sources = get_action('harvest_source_list')(context, {})
            assert_equal(len(sources), 1)
            source_id = sources[0]['id']
        context['detailed'] = True
        context['include_status'] = True  # DGU only
        context['include_job_status'] = True  # DGU only
        harvest_source = logic.get_action('harvest_source_show')(
            context,
            {'id': source_id}
        )
        return harvest_source

    def test_basic(self):
        # The same test as the TestHarvestQueue.test_01_basic_harvester but
        # using job_run.
        source = self._create_source('basic_test', source_type='test')
        job, source = self._job_run(source['id'])

        assert_equal(job['stats'], {'added': 3, 'updated': 0, 'not modified': 0, 'errored': 0, 'deleted': 0})
        assert len(job['objects']) == 3
        assert_equal(source['status']['last_job']['stats'], {'added': 3, 'updated': 0, 'not modified': 0, 'errored': 0, 'deleted': 0})
        assert source['status']['total_datasets'] == 3
        assert source['status']['job_count'] == 1

        # second run
        job, source = self._job_run(source['id'])

        assert_equal(job['stats'], {'added': 0, 'updated': 2, 'not modified': 1, 'errored': 0, 'deleted': 0})
        assert_equal(source['status']['last_job']['stats'], {'added': 0, 'updated': 2, 'not modified': 1, 'errored': 0, 'deleted': 0})
        assert source['status']['total_datasets'] == 2
        assert source['status']['job_count'] == 2

    def test_gather_excepts(self):
        source = self._create_source('gather_excepts', source_type='bad')

        assert_raises(GatherException, self._job_run, source['id'])
        job, source = self._harvest_job_show(), self._harvest_source_show()
        assert job['status'] == 'Aborted'
        assert_equal(job['stats'], {'added': 0, 'updated': 0, 'not modified': 0, 'errored': 0, 'deleted': 0})
        assert source['status']['last_job']['gather_finished']

    def test_gather_excepts_after_creating_objects(self):
        source = self._create_source('gather_excepts_after_creating_objects', source_type='bad')

        assert_raises(GatherException, self._job_run, source['id'])
        job, source = self._harvest_job_show(), self._harvest_source_show()
        assert job['status'] == 'Aborted'
        assert job['objects'] == []

    def test_fetch_excepts(self):
        source = self._create_source('fetch_excepts', source_type='bad')

        self._job_run(source['id'])
        job, source = self._job_run(source['id'])
        assert job['status'] == 'Finished'
        assert job['objects'][0]['state'] == 'ERROR'
        assert job['objects'][0]['report_status'] == 'errored'
        assert job['objects'][0]['fetch_finished']
        assert job['objects'][0]['current'] is False
        assert_equal(source['status']['last_job']['stats'], {'added': 0, 'updated': 0, 'not modified': 0, 'errored': 1, 'deleted': 0})
