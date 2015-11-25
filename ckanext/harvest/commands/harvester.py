import sys
import re
from pprint import pprint

from ckan import model
from ckan.logic import get_action, ValidationError

from ckan.lib.cli import CkanCommand

class Harvester(CkanCommand):
    '''Harvests remotely mastered metadata

    Usage:

      harvester initdb
        - Creates the necessary tables in the database

      harvester source {url} {type} [{active}] [{user-id}] [{publisher-id}]
        - create new harvest source

      harvester clearsource {id}
        - clears all datasets, jobs and objects related to a harvest source, but keeps the source itself

      harvester rmsource {id}
        - remove (inactivate) a harvester source

      harvester sources [all]
        - lists harvest sources
          If 'all' is defined, it also shows the Inactive sources

      harvester job {source-id}
        - create new harvest job and runs it (puts it on the gather queue)

      harvester jobs
        - lists harvest jobs

      harvester job_abort {source-id/name}
        - marks a job as "Aborted" so that the source can be restarted afresh.
          It ensures that the job's harvest objects status are also marked
          finished. You should ensure that neither the job nor its objects are
          currently in the gather/fetch queues.

      harvester run
        - starts any harvest jobs that have been created by putting them onto
          the gather queue. Also checks running jobs - if finished it
          changes their status to Finished.

      harvester gather_consumer
        - starts the consumer for the gathering queue

      harvester fetch_consumer
        - starts the consumer for the fetching queue

      harvester [-j] [--segments={segments}] import [source_id {source-id} | harvest_object_id {object-id} | guid {GUID}]
        - perform the import stage with the last fetched objects, optionally belonging to a certain source
          or object.
          Please note that no objects will be fetched from the remote server. It will only affect
          the last fetched objects already present in the database.

          If the -j flag is provided, the objects are not joined to existing datasets. This may be useful
          when importing objects for the first time.

          The --segments flag allows to define a string containing hex digits that represent which of
          the 16 harvest object segments to import. e.g. 15af will run segments 1,5,a,f

      harvester job-all
        - create new harvest jobs for all active sources.

      harvester job-run {source-id}
        - does a complete harvest synchronously - create job, run, gather & fetch.
          NB Do not run this whilst the fetch or gather processes are running in the background,
          or you'll get duplicate harvest objects and error because the gather & fetch jobs get
          done in the background.

    The commands should be run from the ckanext-harvest directory and expect
    a development.ini file to be present. Most of the time you will
    specify the config explicitly though::

        paster harvester sources --config=../ckan/development.ini

    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 9
    min_args = 0

    def __init__(self,name):

        super(Harvester,self).__init__(name)

        self.parser.add_option('-j', '--no-join-datasets', dest='no_join_datasets',
            action='store_true', default=False, help='Do not join harvest objects to existing datasets')

        self.parser.add_option('--segments', dest='segments',
            default=False, help=
'''A string containing hex digits that represent which of
 the 16 harvest object segments to import. e.g. 15af will run segments 1,5,a,f''')

    def command(self):
        self._load_config()

        # We'll need a sysadmin user to perform most of the actions
        # We will use the sysadmin site user (named as the site_id)
        context = {'model':model,'session':model.Session,'ignore_auth':True}
        self.admin_user = get_action('get_site_user')(context,{})


        print ''

        if len(self.args) == 0:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]
        if cmd == 'source':
            self.create_harvest_source()
        elif cmd == "rmsource":
            self.remove_harvest_source()
        elif cmd == 'clearsource':
            self.clear_harvest_source()
        elif cmd == 'sources':
            self.list_harvest_sources()
        elif cmd == 'job':
            self.create_harvest_job()
        elif cmd == 'jobs':
            self.list_harvest_jobs()
        elif cmd == 'job_abort':
            self.job_abort()
        elif cmd == 'run':
            self.run_harvester()
        elif cmd == 'gather_consumer':
            import logging
            from ckanext.harvest.queue import get_gather_consumer
            logging.getLogger('amqplib').setLevel(logging.INFO)
            consumer = get_gather_consumer()
            logging.getLogger('ckan.cli').info('Now going to wait on the gather queue...')
            consumer.wait()
        elif cmd == 'fetch_consumer':
            import logging
            logging.getLogger('amqplib').setLevel(logging.INFO)
            from ckanext.harvest.queue import get_fetch_consumer
            consumer = get_fetch_consumer()
            logging.getLogger('ckan.cli').info('Now going to wait on the fetch queue...')
            consumer.wait()
        elif cmd == 'initdb':
            self.initdb()
        elif cmd == 'import':
            self.initdb()
            self.import_stage()
        elif cmd == 'job-all':
            self.create_harvest_job_all()
        elif cmd == 'harvesters-info':
            harvesters_info = get_action('harvesters_info_show')()
            pprint(harvesters_info)
        elif cmd == 'job-run':
            self.job_run()
        else:
            print 'Command %s not recognized' % cmd

    def _load_config(self):
        super(Harvester, self)._load_config()

    def initdb(self):
        from ckanext.harvest.model import setup as db_setup
        db_setup()

        print 'DB tables created'

    def create_harvest_source(self):

        if len(self.args) >= 2:
            url = unicode(self.args[1])
        else:
            print 'Please provide a source URL'
            sys.exit(1)
        if len(self.args) >= 3:
            type = unicode(self.args[2])
        else:
            print 'Please provide a source type'
            sys.exit(1)
        if len(self.args) >= 4:
            config = unicode(self.args[3])
        else:
            config = None
        if len(self.args) >= 5:
            active = not(self.args[4].lower() == 'false' or \
                    self.args[4] == '0')
        else:
            active = True
        if len(self.args) >= 6:
            user_id = unicode(self.args[5])
        else:
            user_id = u''
        if len(self.args) >= 7:
            publisher_id = unicode(self.args[6])
        else:
            publisher_id = u''
        try:
            data_dict = {
                    'url':url,
                    'type':type,
                    'config':config,
                    'active':active,
                    'user_id':user_id,
                    'publisher_id':publisher_id}

            context = {'model':model, 'session':model.Session, 'user': self.admin_user['name'],
                    'include_status': True}
            source = get_action('harvest_source_create')(context,data_dict)
            print 'Created new harvest source:'
            self.print_harvest_source(source)

            sources = get_action('harvest_source_list')(context,{})
            self.print_there_are('harvest source', sources)

            # Create a harvest job for the new source
            get_action('harvest_job_create')(
                context, {'source_id': source['id'], 'run': True})
            print 'A new Harvest Job for this source has also been created'
        except ValidationError,e:
           print 'An error occurred:'
           print str(e.error_dict)
           raise e

    def remove_harvest_source(self):
        if len(self.args) >= 2:
            source_id = unicode(self.args[1])
        else:
            print 'Please provide a source id'
            sys.exit(1)
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        get_action('harvest_source_delete')(context,{'id':source_id})
        print 'Removed harvest source: %s' % source_id

    def clear_harvest_source(self):
        if len(self.args) >= 2:
            source_id = unicode(self.args[1])
        else:
            print 'Please provide a source id'
            sys.exit(1)
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        get_action('harvest_source_clear')(context,{'id':source_id})
        print 'Cleared harvest source: %s' % source_id

    def list_harvest_sources(self):
        if len(self.args) >= 2 and self.args[1] == 'all':
            data_dict = {}
            what = 'harvest source'
        else:
            data_dict = {'only_active':True}
            what = 'active harvest source'

        context = {'model': model,'session':model.Session, 'user': self.admin_user['name']}
        sources = get_action('harvest_source_list')(context,data_dict)
        self.print_harvest_sources(sources)
        self.print_there_are(what=what, sequence=sources)

    def create_harvest_job(self):
        if len(self.args) >= 2:
            source_id = unicode(self.args[1])
        else:
            print 'Please provide a source id'
            sys.exit(1)

        context = {'model': model,'session':model.Session, 'user': self.admin_user['name']}
        job = get_action('harvest_job_create')(
                context, {'source_id': source_id, 'run': True})

        self.print_harvest_job(job)
        jobs = get_action('harvest_job_list')(context,{'status':u'New'})
        self.print_there_are('harvest jobs', jobs, condition=u'New')

        return job

    def list_harvest_jobs(self):
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        jobs = get_action('harvest_job_list')(context,{})

        self.print_harvest_jobs(jobs)
        self.print_there_are(what='harvest job', sequence=jobs)

    def job_abort(self):
        if len(self.args) >= 2:
            source_id_or_name = unicode(self.args[1])
        else:
            print 'Please provide a source id'
            sys.exit(1)
        context = {'model': model, 'session': model.Session,
                   'user': self.admin_user['name']}
        source = get_action('harvest_source_show')(
            context, {'id': source_id_or_name})

        context = {'model': model, 'user': self.admin_user['name'],
                   'session': model.Session}
        job = get_action('harvest_job_abort')(context,
                                              {'source_id': source['id']})
        print 'Job status: {0}'.format(job['status'])

    def run_harvester(self):
        context = {'model': model, 'user': self.admin_user['name'],
                   'session': model.Session}
        get_action('harvest_jobs_run')(context, {})

    def import_stage(self):
        id_ = None
        id_types = ('source', 'harvest_object_id', 'guid')
        if len(self.args) == 1:
            # i.e all sources/objects
            pass
        elif len(self.args) == 2:
            print 'ERROR: Specify ID type: %s' % str(id_types)
            sys.exit(1)
        elif len(self.args) == 3:
            id_type = self.args[1]
            if id_type not in id_types:
                print 'ERROR: ID type "%s" not allowed. Choose from: %s' % \
                      (id_type, id_types)
                sys.exit(1)
            if id_type == 'source':
                id_ = unicode(self.args[2])
            elif id_type == 'harvest_object_id':
                id_ = unicode(self.args[2])
            elif id_type == 'guid':
                id_ = unicode(self.args[2])

        context = {'model': model, 'session':model.Session, 'user': self.admin_user['name'],
                   'join_datasets': not self.options.no_join_datasets,
                   'segments': self.options.segments}

        data_dict = {id_type: id_} if id_ else {}
        num_objs = get_action('harvest_objects_import')(context, data_dict)

        print '%s objects reimported' % num_objs

    def create_harvest_job_all(self):
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        jobs = get_action('harvest_job_create_all')(context,{})
        print 'Created %s new harvest jobs' % len(jobs)

    def job_run(self):
        import logging
        from ckan import model
        from ckanext.harvest import queue
        from ckanext.harvest.logic import HarvestJobExists
        from ckanext.harvest.queue import get_gather_consumer, get_fetch_consumer

        logging.getLogger('amqplib').setLevel(logging.INFO)

        source_id = unicode(self.args[1])

        # ensure the queues are empty - needed for this command to run ok
        gather_consumer = get_gather_consumer()
        fetch_consumer = get_fetch_consumer()
        for queue_name, consumer in (('gather', gather_consumer),
                                     ('fetch', fetch_consumer)):
            msg = consumer.fetch()
            if msg:
                print 'Message on %s queue:\n%r' % (queue_name, msg.__dict__)
                resp = raw_input('%s queue is not empty, but needs to be for this command to run. Clear it? (y/n)' % (queue_name.capitalize()))
                if not resp.lower().startswith('y'):
                    sys.exit(1)
                while msg:
                    print 'Delete %s message: %r' % (queue_name, msg)
                    msg.ack()
                    msg = consumer.fetch()

        # get the source id first (source_id may be a source.name)
        context = {'model': model, 'user': self.admin_user['name'],
                   'session': model.Session}
        source = get_action('harvest_source_show')(context,
                                                   {'id': source_id})
        # create harvest job
        try:
            job = get_action('harvest_job_create')(context,
                                                   {'source_id': source['id'],
                                                    'run': False})
        except HarvestJobExists:
            # Job has been created already - we can probably use it.
            # If job status is 'New' then it is ready to run.
            from ckan import model
            context = {'model': model, 'user': self.admin_user['name'],
                       'session': model.Session}
            jobs = get_action('harvest_job_list')(context,
                                                 {'source_id': source['id']})
            job = jobs[0]  # latest one
            if job['status'] != 'New':
                # Non-new status happens when the job is in progress or has
                # gone wrong and is left in limbo, such as during dev work,
                # which is why we access model in this code, rather than have a
                # logic function for it.
                resp = raw_input('The job for this source is in progress or in limbo. Job:%s start:%s status:%s. Start new job?' % (job['id'], job['created'], job['status']))
                if not resp.lower().startswith('y'):
                    sys.exit(1)
                print 'Closing old job cleanly'
                job = get_action('harvest_job_abort')(context,
                                                    {'source_id': source['id']})
                print 'Starting new job'
                job = get_action('harvest_job_create')(context, {'source_id': source['id'],
                                                                 'run': False})

        # run - sends the job to the gather queue
        jobs = get_action('harvest_jobs_run')(context, {'source_id': source['id']})
        assert jobs

        # gather
        logging.getLogger('ckan.cli').info('Gather')
        message = gather_consumer.fetch()
        if not message:
            print 'Could not get gather message - probably because the gather process is running elsewhere.'
            sys.exit(1)
        queue.gather_callback({'harvest_job_id': job['id']}, message)

        # fetch
        logging.getLogger('ckan.cli').info('Fetch')
        while True:
            message = fetch_consumer.fetch()
            if not message:
                break
            queue.fetch_callback(message.payload, message)

        # run - mark the job as finished
        jobs = get_action('harvest_jobs_run')(context, {'source_id': source['id']})

    def print_harvest_sources(self, sources):
        if sources:
            print ''
        for source in sources:
            self.print_harvest_source(source)

    def print_harvest_source(self, source):
        print 'Source id: %s' % source['id']
        print '      url: %s' % source['url']
        print '     type: %s' % source['type']
        print '   active: %s' % source['active']
        print '     user: %s' % source['user_id']
        print 'publisher: %s' % source['publisher_id']
        #print '     jobs: %s' % source['status']['job_count']
        print ''

    def print_harvest_jobs(self, jobs):
        if jobs:
            print ''
        for job in jobs:
            self.print_harvest_job(job)

    def print_harvest_job(self, job):
        print '       Job id: %s' % job['id']
        print '       status: %s' % job['status']
        print '       source: %s' % job['source']
        print '      objects: %s' % len(job['objects'])

        print 'gather_errors: %s' % len(job['gather_errors'])
        if (len(job['gather_errors']) > 0):
            for error in job['gather_errors']:
                print '               %s' % error['message']

        print ''

    def print_there_are(self, what, sequence, condition=''):
        is_singular = self.is_singular(sequence)
        print 'There %s %s %s%s%s' % (
            is_singular and 'is' or 'are',
            len(sequence),
            condition and ('%s ' % condition.lower()) or '',
            what,
            not is_singular and 's' or '',
        )

    def is_singular(self, sequence):
        return len(sequence) == 1

