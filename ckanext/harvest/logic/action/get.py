import logging
from sqlalchemy import or_
from sqlalchemy import distinct

from ckan.common import OrderedDict
from ckan.model import User
import datetime

from ckan.plugins import PluginImplementations
from ckanext.harvest.interfaces import IHarvester

import ckan.plugins as p
from ckan.logic import NotFound, check_access, side_effect_free

from ckanext.harvest import lib as harvest_lib
from ckanext.harvest import model as harvest_model
from ckanext.harvest.model import (HarvestSource, HarvestJob, HarvestObject)
from ckanext.harvest.logic.dictization import (harvest_source_dictize,
                                               harvest_job_dictize,
                                               harvest_object_dictize)

log = logging.getLogger(__name__)


@side_effect_free
def harvest_source_show(context, data_dict):
    '''
    Returns the metadata of a harvest source

    This method just proxies the request to package_show. All auth checks and
    validation will be done there.

    :param id: the id or name of the harvest source
    :type id: string

    :param url: url of the harvest source (as an alternative to the id)
    :type url: string

    :returns: harvest source metadata
    :rtype: dictionary
    '''
    model = context.get('model')

    check_access('harvest_source_show', context, data_dict)

    # Find the source by URL
    if data_dict.get('url') and not data_dict.get('id'):
        source = model.Session.query(harvest_model.HarvestSource) \
                      .filter_by(url=data_dict.get('url')) \
                      .first()
        if not source:
            raise NotFound
        data_dict['id'] = source.id

    id = data_dict.get('id')

    source = HarvestSource.by_name_or_id(id)
    context['source'] = source

    if not source:
        raise NotFound

    data = harvest_source_dictize(source,context)

    if context.get('include_status', True):
        data['status'] = p.toolkit.get_action('harvest_source_show_status')(context, {'id': source.id})
        if context.get('include_job_status', False):
            job_status = p.toolkit.get_action('harvest_source_show_job_status')(context, {'id': source.id})
            data['status'].update(job_status)

    return data


@side_effect_free
def harvest_source_show_status(context, data_dict):
    '''
    Returns a status report for a harvest source.

    Given a particular source, returns a dictionary containing information
    about the source jobs, datasets created, errors, etc.
    Note that this information is already included on the output of
    harvest_source_show, under the 'status' field.

    :param id: the id or name of the harvest source
    :type id: string

    :rtype: dictionary
    '''

    session = context['session']
    p.toolkit.check_access('harvest_source_show_status', context, data_dict)

    model = context.get('model')

    source = harvest_model.HarvestSource.get(data_dict['id'])
    if not source:
        raise p.toolkit.ObjectNotFound('Harvest source {0} does not exist'.format(data_dict['id']))

    out = {
           'total_datasets': 0,
           'datasets': [],
           }

    # Datasets
    packages = model.Session.query(distinct(HarvestObject.package_id),model.Package.name) \
            .join(model.Package).join(HarvestSource) \
            .filter(HarvestObject.source==source) \
            .filter(HarvestObject.current==True) \
            .filter(model.Package.state==u'active')
    out['datasets'] = [package.name for package in packages]

    # Overall statistics
    packages = model.Session.query(model.Package) \
            .join(harvest_model.HarvestObject) \
            .filter(harvest_model.HarvestObject.harvest_source_id==source.id) \
            .filter(harvest_model.HarvestObject.current==True) \
            .filter(model.Package.state==u'active')
    # DGU doesn't have private distinction so we've removed that filter
    out['total_datasets'] = packages.count()

    return out


def harvest_source_show_job_status(context, data_dict):
    '''
    Returns the status of existing jobs and currently running job for a harvest source.

    Side-effect - running jobs will be checked if they are still running and
    flagged if they have now finished.

    :param id: the id or name of the harvest source
    :type id: string

    :rtype: dictionary
    '''

    session = context['session']
    p.toolkit.check_access('harvest_source_show_job_status', context, data_dict)

    source = harvest_model.HarvestSource.get(data_dict['id'])
    if not source:
        raise p.toolkit.ObjectNotFound('Harvest source {0} does not exist'.format(data_dict['id']))

    out = {
           'job_count': 0,
           'last_job': None,
           'next_harvest_job': None,
           'running_job': None,
           }

    jobs = harvest_model.HarvestJob.filter(source=source).all()

    job_count = len(jobs)
    if job_count == 0:
        return out

    out['job_count'] = job_count

    # Get next scheduled job
    next_job = HarvestJob.filter(source=source, status=u'New').first()
    if next_job:
        out['next_harvest_job'] = next_job

    # Get the running or latest job
    last_job = harvest_model.HarvestJob.filter(source=source) \
               .filter(harvest_model.HarvestJob.status != 'New') \
               .order_by(harvest_model.HarvestJob.created.desc()).first()

    if not last_job:
        return out

    context_ = context.copy()
    context_['return_stats'] = True
    context_['return_object_errors'] = True
    context_['return_error_summary'] = True
    out['last_job'] = harvest_job_dictize(last_job, context_)
    running_job = HarvestJob.filter(source=source,status=u'Running') \
                            .order_by(HarvestJob.created.desc()).first()
    if running_job:
        # TODO running_job should really just return data, and leave the
        # presentation to the template
        if running_job.gather_started:
            run_time = datetime.datetime.utcnow() - running_job.gather_started
            minutes, seconds = divmod(run_time.seconds, 60)
            out['running_job'] = '%sm %ss' % (minutes, seconds)
        else:
            out['running_job'] = '(about to start)'
        if not running_job.gather_finished:
            out['running_job'] += ' - gathering records (%s so far)' % \
                                  len(running_job.objects)
        else:
            job_dict = p.toolkit.get_action('harvest_job_show')(context, {'id': running_job.id})
            object_stats = harvest_lib.update_job_status(job_dict, session)
            if object_stats:
                total_objects = sum(object_stats.values())
                state_map = OrderedDict((
                        ('WAITING', 'not yet processed'),
                        ('FETCH', 'in the fetch stage'),
                        ('IMPORT', 'in the import stage'),
                        ('COMPLETE', 'completed ok'),
                        ('ERROR', 'failed'),
                        ))
                object_stats_str = ''.join(
                    ['<br/>%s %s' % (object_stats.get(state_key, '0'), state)
                     for state_key, state in state_map.iteritems()]
                    )
                out['running_job'] += ' - fetch and import of %s records: %s' % \
                       (total_objects, object_stats_str)
            else:
                out['running_job'] += '(now complete)' # i.e. just now complete

    return out

@side_effect_free
def harvest_source_list(context, data_dict):

    user = context.get('user')

    check_access('harvest_source_list',context,data_dict)

    sources = get_sources(context, data_dict)

    return [harvest_source_dictize(source, context) for source in sources]

@side_effect_free
def harvest_source_for_a_dataset(context, data_dict):
    '''
    TODO: Deprecated, harvest source id is added as an extra to each dataset
    automatically
    '''
    '''For a given dataset, return the harvest source that
    created or last updated it, otherwise NotFound.'''

    model = context['model']
    session = context['session']

    dataset_id = data_dict.get('id')

    query = session.query(HarvestSource)\
            .join(HarvestObject)\
            .filter_by(package_id=dataset_id)\
            .order_by(HarvestObject.gathered.desc())
    source = query.first() # newest

    if not source:
        raise NotFound

    if not context.get('include_status'):
        # By default we don't want to know the harvest
        # source status - this is an expensive call.
        context['include_status'] = False
    return harvest_source_dictize(source, context)

@side_effect_free
def harvest_job_show(context,data_dict):

    check_access('harvest_job_show',context,data_dict)

    id = data_dict.get('id')
    attr = data_dict.get('attr',None)

    job = HarvestJob.get(id,attr=attr)
    if not job:
        raise NotFound

    return harvest_job_dictize(job,context)

@side_effect_free
def harvest_job_report(context, data_dict):

    check_access('harvest_job_show', context, data_dict)

    model = context['model']
    id = data_dict.get('id')

    job = HarvestJob.get(id)
    if not job:
        raise NotFound

    report = {
        'gather_errors': [],
        'object_errors': {}
    }

    # Gather errors
    q = model.Session.query(harvest_model.HarvestGatherError) \
                      .join(harvest_model.HarvestJob) \
                      .filter(harvest_model.HarvestGatherError.harvest_job_id==job.id) \
                      .order_by(harvest_model.HarvestGatherError.created.desc())

    for error in q.all():
        report['gather_errors'].append({
            'message': error.message
        })

    # Object errors

    # Check if the harvester for this job's source has a method for returning
    # the URL to the original document
    original_url_builder = None
    for harvester in PluginImplementations(IHarvester):
        if harvester.info()['name'] == job.source.type:
             if hasattr(harvester, 'get_original_url'):
                original_url_builder = harvester.get_original_url

    q = model.Session.query(harvest_model.HarvestObjectError, harvest_model.HarvestObject.guid) \
                      .join(harvest_model.HarvestObject) \
                      .filter(harvest_model.HarvestObject.harvest_job_id==job.id) \
                      .order_by(harvest_model.HarvestObjectError.harvest_object_id)

    for error, guid in q.all():
        if not error.harvest_object_id in report['object_errors']:
            report['object_errors'][error.harvest_object_id] = {
                'guid': guid,
                'errors': []
            }
            if original_url_builder:
                url = original_url_builder(error.harvest_object_id)
                if url:
                    report['object_errors'][error.harvest_object_id]['original_url'] = url

        report['object_errors'][error.harvest_object_id]['errors'].append({
            'message': error.message,
            'line': error.line,
            'type': error.stage
         })

    return report

@side_effect_free
def harvest_job_list(context,data_dict):
    '''Returns a list of jobs and details of objects and errors.
    There is a hard limit of 100 results.

    :param status: filter by e.g. "New" or "Finished" jobs
    :param source_id: filter by a harvest source
    :param offset: paging
    '''

    check_access('harvest_job_list',context,data_dict)

    model = context['model']
    session = context['session']

    source_id = data_dict.get('source_id',False)
    status = data_dict.get('status',False)
    offset = data_dict.get('offset', 0)

    query = session.query(HarvestJob)

    if source_id:
        query = query.filter(HarvestJob.source_id==source_id)

    if status:
        query = query.filter(HarvestJob.status==status)

    query = query.order_by(HarvestJob.created.desc())

    # Have a max for safety
    query = query.offset(offset).limit(100)

    jobs = query.all()

    context['return_error_summary'] = False
    context['return_stats'] = False
    return [harvest_job_dictize(job,context) for job in jobs]

@side_effect_free
def harvest_object_show(context,data_dict):
    p.toolkit.check_access('harvest_object_show', context, data_dict)

    id = data_dict.get('id')
    dataset_id = data_dict.get('dataset_id')

    if id:
        attr = data_dict.get('attr',None)
        obj = HarvestObject.get(id,attr=attr)
    elif dataset_id:
        model = context['model']

        pkg = model.Package.get(dataset_id)
        if not pkg:
            raise p.toolkit.ObjectNotFound('Dataset not found')

        obj = model.Session.query(HarvestObject) \
              .filter(HarvestObject.package_id == pkg.id) \
              .filter(HarvestObject.current == True) \
              .first()
    else:
        raise p.toolkit.ValidationError(
            'Please provide either an "id" or a "dataset_id" parameter')

    if not obj:
        raise p.toolkit.ObjectNotFound('Harvest object not found')

    return harvest_object_dictize(obj,context)

@side_effect_free
def harvest_object_list(context,data_dict):

    check_access('harvest_object_list',context,data_dict)

    model = context['model']
    session = context['session']

    only_current = data_dict.get('only_current',True)
    source_id = data_dict.get('source_id',False)

    query = session.query(HarvestObject)

    if source_id:
        query = query.filter(HarvestObject.source_id==source_id)

    if only_current:
        query = query.filter(HarvestObject.current==True)

    objects = query.all()

    return [getattr(obj,'id') for obj in objects]

@side_effect_free
def harvesters_info_show(context,data_dict):
    '''Returns details of the installed harvesters.'''
    check_access('harvesters_info_show',context,data_dict)

    available_harvesters = []
    for harvester in PluginImplementations(IHarvester):
        info = harvester.info()
        if not info or 'name' not in info:
            log.error('Harvester %r does not provide the harvester name in the info response' % str(harvester))
            continue
        info['show_config'] = (info.get('form_config_interface','') == 'Text')
        available_harvesters.append(info)

    return available_harvesters


def get_sources(context, data_dict):

    model = context['model']
    session = context['session']
    user = context.get('user','')

    only_mine = data_dict.get('only_mine', False)
    only_active = data_dict.get('only_active',False)
    only_organization = data_dict.get('organization') or data_dict.get('group')
    only_to_run = data_dict.get('only_to_run',False)

    query = session.query(HarvestSource) \
                .order_by(HarvestSource.created.desc())

    if only_active:
        query = query.filter(HarvestSource.active==True) \

    if only_to_run:
        query = query.filter(HarvestSource.frequency!='MANUAL')
        query = query.filter(or_(HarvestSource.next_run<=datetime.datetime.utcnow(),
                                 HarvestSource.next_run==None)
                            )
    user_obj = User.get(user)
    if user_obj and not user_obj.sysadmin and only_mine:
        # Filter by the user's organizations

        publisher_filters = []
        data_dict_ = {'permission': 'create_dataset'}
        publishers_for_the_user = \
            p.toolkit.get_action('organization_list_for_user')(context, data_dict_)
        for publisher_id in [g['id'] for g in publishers_for_the_user]:
            publisher_filters.append(HarvestSource.publisher_id==publisher_id)

        if len(publisher_filters):
            query = query.filter(or_(*publisher_filters))
        else:
            # This user does not belong to a publisher yet, no sources for him/her
            return []

        log.debug('User %s with publishers %r has Harvest Sources: %r',
                  user, publishers_for_the_user, [(hs.id, hs.url) for hs in query])

    if only_organization:
        org = model.Group.get(only_organization)
        if not org:
            raise p.toolkit.ObjectNotFound('Could not find: %s' % only_organization)
        query = query.filter(HarvestSource.publisher_id==org.id)

    sources = query.all()

    return sources

