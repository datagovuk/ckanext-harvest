from ckan.plugins import toolkit as pt
from ckan.lib.base import _
import ckan.new_authz

from ckanext.harvest.model import HarvestSource

def harvest_source_create(context, data_dict):
    '''
        Authorization check for harvest source creation

        It forwards the checks to package_create, which will check for
        organization membership, whether if sysadmin, etc according to the
        instance configuration.
    '''
    user = context.get('user')
    try:
        pt.check_access('package_create', context, data_dict)
        return {'success': True}
    except pt.NotAuthorized:
        return {'success': False,
                'msg': pt._('User {0} not authorized to create harvest sources').format(user)}

def harvest_job_create(context, data_dict):
    '''
        Authorization check for harvest job creation

        It forwards the checks to package_update, ie the user can only create
        new jobs if she is allowed to edit the harvest source's dataset.
    '''
    model = context['model']
    source_id = data_dict['source_id']
    user = context.get('user')

    source = HarvestSource.by_name_or_id(source_id)
    if not source:
        raise pt.ObjectNotFound(pt._('Harvest source not found'))

    check = ckan.new_authz.has_user_permission_for_group_or_org(
        source.publisher_id, user, 'update_dataset'
    )
    if not check:
        return {'success': False,
                'msg': _('User %s not authorized to edit these groups') %
                        (str(user))}
    return {'success': True}

def harvest_job_create_all(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not ckan.new_authz.is_sysadmin(user):
        return {'success': False, 'msg': _('Only sysadmins can create harvest jobs for all sources') % str(user)}
    else:
        return {'success': True}


def harvest_object_create(context, data_dict):
    """
        Auth check for creating a harvest object

        only the sysadmins can create harvest objects
    """
    # sysadmins can run all actions if we've got to this point we're not a sysadmin
    return {'success': False, 'msg': pt._('Only the sysadmins can create harvest objects')}
