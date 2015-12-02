import logging
from sqlalchemy import and_, func

from pylons import url as _pylons_default_url
from ckanext.harvest.model import HarvestJob, HarvestObject

log = logging.getLogger(__name__)


def pager_url(ignore=None, page=None):
    # This has params the same as ckan.controllers.Package.pager_url NOT
    # ckan.helpers.pager_url
    routes_dict = _pylons_default_url.environ['pylons.routes_dict']
    kwargs = {}
    kwargs['controller'] = routes_dict['controller']
    kwargs['action'] = routes_dict['action']
    if routes_dict.get('id'):
        kwargs['id'] = routes_dict['id']
    kwargs['page'] = page
    return _pylons_default_url(**kwargs)


def update_job_status(job, session):
    '''For a given job, this will check to see if it is finished and update the
    job.status if necessary. If in the fetch/import stage then it will return
    the state counts of the job's harvest objects.

    @param job: job dictionary
    @return None unless it is in the fetch/import stage and it will return the
        counts of HarvestObject states as a dict:
        {'ERROR': 5, 'COMPLETE': 10, 'WAITING': 3}

    '''
    if job['status'] in ('Finished', 'Aborted', 'New') or not job['gather_finished']:
        return
    # Must be in the fetch/import stage. job['status'] == 'Running'
    # See how far it is through its HarvestObjects
    unprocessed_objects = session.query(HarvestObject.id) \
        .filter(HarvestObject.harvest_job_id == job['id']) \
        .filter(~HarvestObject.state.in_((u'COMPLETE', 'ERROR', 'ABORTED')))

    if unprocessed_objects.count() != 0:
        # job not finished yet - report the state counts
        object_states = session.query(
            HarvestObject.state,
            func.count(HarvestObject.state).label('count')
            ) \
            .filter(HarvestObject.harvest_job_id == job['id']) \
            .group_by(HarvestObject.state)
        object_states_dict = dict((os.state, os.count)
                                    for os in object_states)
        log.debug('HarvestJob not yet finished - %s %s',
                  job['id'], object_states_dict)
        return object_states_dict

    # job is now finished
    log.debug('HarvestJob finished - job:%s source:%s', job['id'], job['source_id'])
    job_obj = HarvestJob.get(job['id'])
    job_obj.status = u'Finished'
    log.debug('HarvestJob report statuses: %r',
              job_obj.get_object_report_statuses())

    # record when it was finished
    last_object = session.query(HarvestObject) \
        .filter(HarvestObject.harvest_job_id == job['id']) \
        .filter(HarvestObject.import_finished != None) \
        .order_by(HarvestObject.import_finished.desc()) \
        .first()
    if last_object:
        job_obj.finished = last_object.import_finished
    job_obj.save()
    # DGU Hack - we don't have harvest_sources in solr so it's commented out
    ## Reindex the harvest source dataset so it has the latest
    ## status
    #get_action('harvest_source_reindex')(context,
    #    {'id': job_obj.source.id})
