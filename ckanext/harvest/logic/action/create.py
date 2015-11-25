import re
import logging

import ckan
from ckan.plugins import toolkit

from ckanext.harvest.logic import HarvestJobExists
from ckan.lib.navl.dictization_functions import validate

from ckanext.harvest.model import (HarvestSource, HarvestJob, HarvestObject,
    HarvestObjectExtra)
from ckanext.harvest.logic.schema import default_harvest_source_schema
from ckanext.harvest.logic.dictization import (harvest_source_dictize,
                                               harvest_job_dictize,
                                               harvest_object_dictize)
from ckanext.harvest.logic.schema import harvest_object_create_schema
from ckanext.harvest.logic.action.get import harvest_source_list,harvest_job_list
from ckanext.harvest.lib import HarvestError
check_access = toolkit.check_access

_validate = ckan.lib.navl.dictization_functions.validate
log = logging.getLogger(__name__)


def harvest_source_create(context,data_dict):

    log.info('Creating harvest source: %r', data_dict)
    check_access('harvest_source_create',context,data_dict)

    model = context['model']
    session = context['session']
    schema = context.get('schema') or default_harvest_source_schema()

    data, errors = validate(data_dict, schema, context=context)

    if errors:
        session.rollback()
        log.warn('Harvest source does not validate: %r', errors)
        raise toolkit.ValidationError(errors,_error_summary(errors))

    source = HarvestSource()
    source.url = data['url'].strip()
    source.type = data['type']

    opt = ['active','title','description','user_id','publisher_id','config','frequency','name']
    for o in opt:
        if o in data and data[o] is not None:
            source.__setattr__(o,data[o])

    if 'active' in data_dict:
        source.active = data['active']

    source.save()
    log.info('Harvest source created: %s', source.id)

    return harvest_source_dictize(source,context)

def harvest_job_create(context, data_dict):
    '''
    Creates a Harvest Job for a Harvest Source and runs it (by putting it on
    the gather queue)

    :param source_id:
    :type source_id: string
    :param run: whether to also run it or not (default: True)
    :type run: bool
    '''
    log.info('Harvest job create: %r', data_dict)
    check_access('harvest_job_create', context, data_dict)

    source_id = data_dict['source_id']
    run_it = data_dict.get('run', True)

    # Check if source exists
    source = HarvestSource.by_name_or_id(source_id)
    if not source:
        log.warn('Harvest source %s does not exist', source_id)
        raise toolkit.NotFound('Harvest source %s does not exist' % source_id)

    # Check if the source is active
    if not source.active:
        log.warn('Harvest job cannot be created for inactive source %s',
                 source_id)
        raise HarvestError('Can not create jobs on inactive sources')

    # Check if there already is an unrun or currently running job for this source
    exists = _check_for_existing_jobs(context, source.id)
    if exists:
        log.warn('There is already an unrun job %r for this source %s',
                 exists, source_id)
        raise HarvestJobExists('There already is an unrun job for this source')

    job = HarvestJob()
    job.source = source

    job.save()
    log.info('Harvest job saved %s', job.id)

    if run_it:
        toolkit.get_action('harvest_send_job_to_gather_queue')(
            context, {'id': job.id})

    return harvest_job_dictize(job, context)


def harvest_job_create_all(context, data_dict):
    '''
    Creates a Harvest Job for all Harvest Sources and runs them (by
    putting them on the gather queue)

    :param source_id:
    :type source_id: string
    :param run: whether to also run the jobs or not (default: True)
    :type run: bool
    '''

    log.info('Harvest job create all: %r', data_dict)
    check_access('harvest_job_create_all',context,data_dict)

    run = data_dict.get('run', True)

    data_dict.update({'only_active':True})

    # Get all active sources
    sources = harvest_source_list(context,data_dict)
    jobs = []
    # Create a new job for each, if there isn't already one
    for source in sources:
        exists = _check_for_existing_jobs(context, source['id'])
        if exists:
            log.info('Skipping source %s as it already has a pending job', source['id'])
            continue

        job = harvest_job_create(
            context, {'source_id': source['id'], 'run': run})
        jobs.append(job)

    log.info('Created jobs for %s%i harvest sources',
             'and run ' if run else '', len(jobs))
    return jobs

def _check_for_existing_jobs(context, source_id):
    '''
    Given a source id, checks if there are jobs for this source
    with status 'New' or 'Running'

    rtype: boolean
    '''
    data_dict ={
        'source_id':source_id,
        'status':u'New'
    }
    exist_new = harvest_job_list(context,data_dict)
    data_dict ={
        'source_id':source_id,
        'status':u'Running'
    }
    exist_running = harvest_job_list(context,data_dict)
    exist = len(exist_new + exist_running) > 0

    return exist

def harvest_object_create(context, data_dict):
    ''' Create a new harvest object

    :type guid: string (optional)
    :type content: string (optional)
    :type job_id: string
    :type source_id: string (optional)
    :type package_id: string (optional)
    :type extras: dict (optional)
    '''
    check_access('harvest_object_create', context, data_dict)
    data, errors = _validate(data_dict, harvest_object_create_schema(), context)

    if errors:
        raise toolkit.ValidationError(errors)

    obj = HarvestObject(
        guid=data.get('guid'),
        content=data.get('content'),
        job=data['job_id'],
        harvest_source_id=data.get('source_id'),
        package_id=data.get('package_id'),
        extras=[ HarvestObjectExtra(key=k, value=v) 
            for k, v in data.get('extras', {}).items() ]
    )

    obj.save()
    return harvest_object_dictize(obj, context)

def _error_summary(error_dict):
    error_summary = {}
    for key, error in error_dict.iteritems():
        error_summary[_prettify(key)] = error[0]
    return error_summary

def _prettify(field_name):
    field_name = re.sub('(?<!\w)[Uu]rl(?!\w)', 'URL', field_name.replace('_', ' ').capitalize())
    return field_name.replace('_', ' ')
