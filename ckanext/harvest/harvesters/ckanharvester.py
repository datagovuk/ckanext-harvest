import urllib2
import dateutil.parser

from paste.deploy.converters import asbool

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json
from ckan.plugins.core import implements

from ckanext.harvest.model import HarvestJob, HarvestObject, \
                                  HarvestObjectError, HarvestObjectExtra
from ckanext.harvest.interfaces import IHarvester

import logging
log = logging.getLogger(__name__)

from base import HarvesterBase


class CKANHarvester(HarvesterBase):
    '''
    A Harvester for CKAN instances
    '''
    implements(IHarvester)

    config = None

    api_version = 2
    ckan_revision_api_works = True

    def _get_rest_api_offset(self):
        return '/api/%d/rest' % self.api_version

    def _get_search_api_offset(self):
        return '/api/%d/search' % self.api_version

    def _get_content(self, url):
        http_request = urllib2.Request(
            url = url,
        )

        api_key = self.config.get('api_key',None)
        if api_key:
            http_request.add_header('Authorization',api_key)
        http_response = urllib2.urlopen(http_request)

        return http_response.read()

    def _get_group(self, base_url, group_name):
        url = base_url + self._get_rest_api_offset() + '/group/' + group_name
        try:
            content = self._get_content(url)
            return json.loads(content)
        except Exception, e:
            raise e

    def _set_config(self,config_str):
        if config_str:
            self.config = json.loads(config_str)
            if self.config.get('api_version'):
                self.api_version = int(self.config['api_version'])
            self.organizations_include = self.config.get('organizations_filter_include', [])
            self.organizations_exclude = self.config.get('organizations_filter_exclude', [])
            log.debug('Using config: %r', self.config)
        else:
            self.config = {}
            self.organizations_include = []
            self.organizations_exclude = []

    def info(self):
        return {
            'name': 'ckan',
            'title': 'CKAN',
            'description': 'Harvests remote CKAN instances',
            'form_config_interface':'Text'
        }

    def validate_config(self,config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'api_version' in config_obj:
                try:
                    int(config_obj['api_version'])
                except ValueError:
                    raise ValueError('api_version must be an integer')

            if 'organizations_filter_include' in config_obj:
                if not isinstance(config_obj['organizations_filter_include'],list):
                    raise ValueError('organizations_filter_include must be a list')

            if 'organizations_filter_exclude' in config_obj:
                if not isinstance(config_obj['organizations_filter_exclude'],list):
                    raise ValueError('organizations_filter_exclude must be a list')

            if 'default_tags' in config_obj:
                if not isinstance(config_obj['default_tags'],list):
                    raise ValueError('default_tags must be a list')

            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'],list):
                    raise ValueError('default_groups must be a list')

                # Check if default groups exist
                context = {'model':model,'user':c.user}
                for group_name in config_obj['default_groups']:
                    try:
                        group = get_action('group_show')(context,{'id':group_name})
                    except NotFound,e:
                        raise ValueError('Default group not found')

            if 'default_extras' in config_obj:
                if not isinstance(config_obj['default_extras'],dict):
                    raise ValueError('default_extras must be a dictionary')

            if 'user' in config_obj:
                # Check if user exists
                context = {'model':model,'user':c.user}
                try:
                    user = get_action('user_show')(context,{'id':config_obj.get('user')})
                except NotFound,e:
                    raise ValueError('User not found')

            for key in ('force_all'):
                if key in config_obj:
                    if not isinstance(config_obj[key],bool):
                        raise ValueError('%s must be boolean' % key)

        except ValueError,e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.gather')
        log.debug('In CKANHarvester gather_stage (%s)', harvest_job.source.url)
        get_all_packages = True
        package_ids = []

        self._set_config(harvest_job.source.config)

        # Check if this source has been harvested before
        previous_job = Session.query(HarvestJob) \
                        .filter(HarvestJob.source==harvest_job.source) \
                        .filter(HarvestJob.gather_finished!=None) \
                        .filter(HarvestJob.id!=harvest_job.id) \
                        .order_by(HarvestJob.gather_finished.desc()) \
                        .limit(1).first()

        # Get source URL
        base_url = harvest_job.source.url.rstrip('/')
        base_rest_url = base_url + self._get_rest_api_offset()
        base_search_url = base_url + self._get_search_api_offset()

        # Under normal circumstances we can just get the packages modified
        # since the last job
        if (self.ckan_revision_api_works and
                previous_job and not previous_job.gather_errors and
                len(previous_job.objects)):
            if not self.config.get('force_all'):
                get_all_packages = False

                # Request only the packages modified since last harvest job
                last_time = previous_job.gather_finished.isoformat()
                url = base_search_url + '/revision?since_time=%s' % last_time
                # NB CKAN API v3 doesn't appear to have an equivalent yet (!)
                # but try this one anyway, as it falls back to the next method.

                try:
                    log.debug('Trying revision API: %s', url)
                    content = self._get_content(url)

                    revision_ids = json.loads(content)
                    log.debug('Revision API returned %s revisions', len(revision_ids))
                    if len(revision_ids):
                        for revision_id in revision_ids:
                            url = base_rest_url + '/revision/%s' % revision_id
                            try:
                                content = self._get_content(url)
                            except Exception, e:
                                self.save_gather_error(
                                    'Unable to get content for URL: %s: %s' %
                                    (url, e), harvest_job)
                                continue

                            revision = json.loads(content)
                            for package_id in revision['packages']:
                                if not package_id in package_ids:
                                    package_ids.append(package_id)
                        log.debug('Revision API returned %s datasets', len(package_ids))
                        # NB Some of these datasets may have been deleted, so
                        # you'll get a 403 error when you try to access it
                        # later on.
                    else:
                        log.info('No packages have been updated on the remote CKAN instance since the last harvest job')
                        return None

                except urllib2.HTTPError, e:
                    if e.getcode() == 400:
                        log.info('CKAN instance %s does not suport revision filtering' % base_url)
                        get_all_packages = True
                    else:
                        self.save_gather_error(
                            'Unable to get content for URL: %s: %s' %
                            (url, e), harvest_job)
                        return None
                except json.decoder.JSONDecodeError:
                    log.info('CKAN instance %s does not suport revision filtering' % base_url)
                    get_all_packages = True

        # It wasn't possible to get just the latest changed datasets, so simply
        # get all of them.
        if get_all_packages:
            package_ids = self._get_all_packages(base_url, harvest_job)
            if package_ids is None:
                # gather_error already saved
                return None
        try:
            object_ids = []
            if not package_ids:
                self.save_gather_error('No datasets listed',
                                       harvest_job)
                return None

            # Create objects for each dataset at the remote CKAN
            for package_id in package_ids:
                # Create a new HarvestObject for this identifier
                obj = HarvestObject(guid=package_id, job=harvest_job)
                obj.save()
                object_ids.append(obj.id)

            return object_ids

        except Exception, e:
            self.save_gather_error('%r' % e.message, harvest_job)

    def _get_all_packages(self, base_url, harvest_job):
        log = logging.getLogger(__name__ + '.gather')
        '''Request the IDs of all remote packages'''
        url = base_url + self._get_rest_api_offset() + '/package'
        log.debug('Getting list of datasets: %s', url)
        try:
            content = self._get_content(url)
        except Exception, e:
            self._save_gather_error('Unable to get content for URL: %s - %s'
                                    % (url, e), harvest_job)
            return None

        try:
            return json.loads(content)
        except ValueError, e:
            self._save_gather_error('Unable to parse response as JSON. Response starts: %r Error: %s'
                    % (content[:100], e), harvest_job)
            return None

    def fetch_stage(self, harvest_object):
        log = logging.getLogger(__name__ + '.fetch')
        log.debug('In CKANHarvester fetch_stage')

        self._set_config(harvest_object.job.source.config)

        # Get source URL
        base_url = harvest_object.source.url.rstrip('/')
        url, content = self._get_package(base_url, harvest_object)
        if content is None:
            # _get_package has already saved an object_error
            return False

        try:
            dataset = json.loads(content)
        except ValueError, e:
            self.save_object_error(
                'CKAN content could not be deserialized: %s: %r' % (url, e),
                harvest_object)
            return False

        # If configuration contains a list of organizations we want to restrict
        # the harvester to, then check this dataset is in one of those orgs.
        if 'organization' in dataset:
            org_name = dataset['organization'].get('name', '')
            if org_name:
                if self.organizations_include:
                    if not org_name in self.organizations_include:
                        log.debug('Skipping dataset %s as organization %s not in organizations_filter_include' % (dataset['name'], org_name,))
                        return 'unchanged'
                if self.organizations_exclude:
                    if org_name in self.organizations_exclude:
                        log.debug('Skipping dataset %s as organisation %s is in organizations_filter_exclude' % (dataset['name'], org_name,))
                        return 'unchanged'

        # Skip datasets that are flagged dgu_harvest_me=false
        ignore_dataset = False
        if isinstance(dataset.get('extras'), dict):
            # CKAN API v2
            if asbool(dataset['extras'].get('dgu_harvest_me') or True) is False:
                ignore_dataset = True
        elif isinstance(dataset.get('extras'), list):
            # CKAN API v3
            for extra in dataset['extras']:
                if extra['key'] == 'dgu_harvest_me' and \
                        asbool(extra['value']) is False:
                    ignore_dataset = True
                    break
        if ignore_dataset:
            log.debug('Skipping dataset %s as required by the dgu_harvest_me extra' % dataset['name'])
            return 'unchanged'

        # Save the fetched contents in the HarvestObject
        harvest_object.content = content
        harvest_object.save()

        # Extract the modification date
        modified = dataset.get('metadata_modified')
        # e.g. "2014-05-10T02:22:05.483412"
        if not modified:
            self.save_object_error(
                'CKAN content did not have metadata_modified: %s' % url,
                harvest_object)
            return False
        try:
            modified = dateutil.parser.parse(modified)
        except ValueError:
            self.save_object_error(
                'CKAN modified date did not parse: %s url: %s' % (modified, url),
                harvest_object)
            return False


        # Set the HarvestObjectExtra.status
        previous_obj = model.Session.query(HarvestObject) \
                            .filter_by(guid=harvest_object.guid) \
                            .filter_by(current=True) \
                            .first()
        if previous_obj:
            # See if the object has changed
            previous_modified = previous_obj.metadata_modified_date or \
                previous_obj.get_extra('modified')
                # look in the extra for backward compatibility only
            if not self.config.get('force_all'):
                if previous_modified == modified:
                    log.info('Skipping unchanged package with GUID %s' %
                            harvest_object.guid)
                    return 'unchanged'  # it will not carry on to import_stage

                if previous_modified > modified:
                    self.save_object_error('CKAN modification date is earlier than when it was last harvested! %s Last harvest: %s This harvest: %s' %
                                            (url, previous_modified, modified),
                                            harvest_object)
            log.info('Package with GUID %s exists and needs to be updated' %
                     harvest_object.guid)
            status = 'changed'
        else:
            status = 'new'

        harvest_object.metadata_modified_date = modified
        harvest_object.extras.append(HarvestObjectExtra(key='status', value=status))
        harvest_object.extras.append(HarvestObjectExtra(key='url', value=url))
        harvest_object.save()

        return True

    def _get_package(self, base_url, harvest_object):
        url = base_url + self._get_rest_api_offset() + '/package/' + harvest_object.guid

        # Get contents
        try:
            return url, self._get_content(url)
        except Exception, e:
            self._save_object_error(
                'Unable to get content for package: %s: %s' % (url, e),
                harvest_object)
            return None, None

    @classmethod
    def get_harvested_package_dict(cls, harvest_object):
        '''Returns the remote package_dict. This method only exists so that
        dkanharvester can override it and convert the DKAN-style bits into
        CKAN-style.
        '''
        try:
            package_dict_harvested = json.loads(harvest_object.content)
        except ValueError, e:
            cls.save_object_error('CKAN content could not be deserialized: %s: %r' % \
                                    (harvest_object.url, e), harvest_object)
            return None
        return package_dict_harvested

    def get_package_dict(self, harvest_object, package_dict_defaults,
                         source_config, existing_dataset):
        log = logging.getLogger(__name__ + '.import.get_package_dict')
        package_dict_harvested = self.get_harvested_package_dict(harvest_object)
        if package_dict_harvested is None:
            return

        package_dict = package_dict_defaults.merge(package_dict_harvested)
        override_extras = source_config.get('override_extras', False)
        if override_extras:
            package_dict['extras'].update(package_dict_defaults['extras'])
        source_config['clean_tags'] = True

        # name - ignore the default value because we should try and use the
        # harvested name if possible.
        package_dict['name'] = package_dict_harvested.get('name') or \
                               self._gen_new_name(package_dict['title'])
        if not existing_dataset:
            package_dict['name'] = self._check_name(package_dict['name'])
        elif package_dict['name'] != existing_dataset.name:
            package_dict['name'] = self._check_name(package_dict['name'],
                                        existing_name=existing_dataset.name)

        if package_dict.get('type') == 'harvest':
            log.debug('Remote dataset is a harvest source, ignoring...')
            return

        remote_groups = source_config.get('remote_groups', None)
        if not remote_groups in ('only_local', 'create'):
            # Ignore remote groups
            package_dict.pop('groups', None)
        else:
            if not 'groups' in package_dict:
                package_dict['groups'] = []

            # check if remote groups exist locally, otherwise remove
            validated_groups = []
            context = {'model': model, 'session': Session, 'user': 'harvest'}

            for group_name in package_dict['groups']:
                try:
                    data_dict = {'id': group_name}
                    group = get_action('group_show')(context, data_dict)
                    if self.api_version == 1:
                        validated_groups.append(group['name'])
                    else:
                        validated_groups.append(group['id'])
                except NotFound, e:
                    log.info('Group %s is not available' % group_name)
                    if remote_groups == 'create':
                        try:
                            group = self._get_group(harvest_object.source.url, group_name)
                        except:
                            log.error('Could not get remote group %s' % group_name)
                            continue

                        for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name']:
                            group.pop(key, None)
                        get_action('group_create')(context, group)
                        log.info('Group %s has been newly created' % group_name)
                        if self.api_version == 1:
                            validated_groups.append(group['name'])
                        else:
                            validated_groups.append(group['id'])

            package_dict['groups'] = validated_groups

        context = {'model': model, 'session': Session, 'user': 'harvest'}

        # Local harvest source organization
        local_org = harvest_object.source.publisher_id

        remote_orgs = source_config.get('remote_orgs', None)

        if not remote_orgs in ('only_local', 'create'):
            # Assign dataset to the source organization
            package_dict['owner_org'] = local_org
        else:
            if not 'owner_org' in package_dict:
                package_dict['owner_org'] = None

            # check if remote org exist locally, otherwise remove
            validated_org = None
            remote_org = package_dict['owner_org']

            if remote_org:
                try:
                    data_dict = {'id': remote_org}
                    org = get_action('organization_show')(context, data_dict)
                    validated_org = org['id']
                except NotFound, e:
                    log.info('Organization %s is not available' % remote_org)
                    if remote_orgs == 'create':
                        try:
                            org = self._get_group(harvest_object.source.url, remote_org)
                            for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name', 'type']:
                                org.pop(key, None)
                            get_action('organization_create')(context, org)
                            log.info('Organization %s has been newly created' % remote_org)
                            validated_org = org['id']
                        except:
                            log.error('Could not get remote org %s' % remote_org)

            package_dict['owner_org'] = validated_org or local_org

        # Metadata provenance
        package_dict['extras']['metadata_provenance'] = self.get_metadata_provenance(
            harvested_provenance=package_dict_harvested['extras'].get('metadata_provenance'),
            harvest_object=harvest_object)

        # DGU local authority field fix - Barnet uses different key
        la_service = package_dict['extras'].get('Service type URI')
        if la_service:
            package_dict['extras']['la_service'] = la_service

        # Find any extras whose values are not strings and try to convert
        # them to strings, as non-string extras are not allowed anymore in
        # CKAN 2.0.
        for key in package_dict['extras'].keys():
            if not isinstance(package_dict['extras'][key], basestring):
                try:
                    package_dict['extras'][key] = json.dumps(
                            package_dict['extras'][key])
                except TypeError:
                    # If converting to a string fails, just delete it.
                    del package_dict['extras'][key]

        # DGU ONLY: Guess theme from other metadata
        try:
            from ckanext.dgu.lib.theme import categorize_package, PRIMARY_THEME, SECONDARY_THEMES
            themes = categorize_package(package_dict)
            if themes:
                package_dict['extras'][PRIMARY_THEME] = themes[0]
                package_dict['extras'][SECONDARY_THEMES] = json.dumps(themes[1:])
        except ImportError:
            pass

        # Convert dicts to lists (required for package_create/update)
        package_dict['extras'] = [dict(key=key, value=package_dict['extras'][key])
                                    for key in package_dict['extras']]

        self._fix_tags(package_dict)

        for resource in package_dict.get('resources', []):
            # Clear remote url_type for resources (eg datastore, upload) as we
            # are only creating normal resources with links to the remote ones
            resource.pop('url_type', None)
            # Details of the upload are irrelevant to this CKAN, so strip that
            if resource.get('resource_type') == 'file.upload':
                resource['resource_type'] = 'file'

        return package_dict

    def _fix_tags(self, package_dict):
        package_dict['tags'] = [dict(name=name)
                                for name in package_dict['tags']]
