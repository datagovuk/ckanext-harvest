import logging
import datetime
import re
import uuid

from sqlalchemy.sql import update, bindparam
from pylons import config

from ckan import plugins as p
from ckan import model
from ckan.model import Session, Package, PACKAGE_NAME_MAX_LENGTH
from ckan.lib import maintain
from ckan.logic import ValidationError, NotFound, get_action
from ckan.logic.schema import default_create_package_schema
from ckan.lib.navl.validators import ignore_missing, ignore
from ckan.lib.munge import munge_title_to_name, substitute_ascii_equivalents


from ckanext.harvest.model import HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckan.lib.helpers import json

log = logging.getLogger(__name__)


if p.toolkit.check_ckan_version(min_version='2.3'):
    from ckan.lib.munge import munge_tag
else:
    # Fallback munge_tag for older ckan versions which don't have a decent
    # munger
    def _munge_to_length(string, min_length, max_length):
        '''Pad/truncates a string'''
        if len(string) < min_length:
            string += '_' * (min_length - len(string))
        if len(string) > max_length:
            string = string[:max_length]
        return string

    def munge_tag(tag):
        tag = substitute_ascii_equivalents(tag)
        tag = tag.lower().strip()
        tag = re.sub(r'[^a-zA-Z0-9\- ]', '', tag).replace(' ', '-')
        tag = _munge_to_length(tag, model.MIN_TAG_LENGTH, model.MAX_TAG_LENGTH)
        return tag


def munge_tags(package_dict):
    tags = package_dict.get('tags', [])
    tags = [munge_tag(t['name']) for t in tags if t]
    tags = [t for t in tags if t != '__']  # i.e. just padding
    tags = remove_duplicates_in_a_list(tags)
    package_dict['tags'] = [dict(name=name) for name in tags]


def remove_duplicates_in_a_list(list_):
    seen = []
    seen_add = seen.append
    return [x for x in list_ if not (x in seen or seen_add(x))]


class HarvesterBase(SingletonPlugin):
    '''
    Generic base class for harvesters, providing a number of useful functions.

    A harvester doesn't have to derive from this - it could just have:

        implements(IHarvester)
    '''
    implements(IHarvester)
    config = None

    _user_name = None

    @classmethod
    def _gen_new_name(cls, title, existing_name=None,
                      append_type='number-sequence'):
        '''
        Returns a 'name' for the dataset (URL friendly), based on the title.

        If the ideal name is already used, it will append a number to it to
        ensure it is unique.

        If generating a new name because the title of the dataset has changed,
        specify the existing name, in case the name doesn't need to change
        after all.

        :param existing_name: the current name of the dataset - only specify
                              this if the dataset exists
        :type existing_name: string
        :param append_type: the type of characters to add to make it unique -
                            either 'number-sequence' or 'random-hex'.
        :type append_type: string
        '''

        ideal_name = munge_title_to_name(title)
        ideal_name = re.sub('-+', '-', ideal_name)  # collapse multiple dashes
        return cls._ensure_name_is_unique(ideal_name,
                                          existing_name=existing_name,
                                          append_type=append_type)

    @staticmethod
    def _ensure_name_is_unique(ideal_name, existing_name=None,
                               append_type='number-sequence'):
        '''
        Returns a dataset name based on the ideal_name, only it will be
        guaranteed to be different than all the other datasets, by adding a
        number on the end if necessary.

        If generating a new name because the title of the dataset has changed,
        specify the existing name, in case the name doesn't need to change
        after all.

        The maximum dataset name length is taken account of.

        :param ideal_name: the desired name for the dataset, if its not already
                           been taken (usually derived by munging the dataset
                           title)
        :type ideal_name: string
        :param existing_name: the current name of the dataset - only specify
                              this if the dataset exists
        :type existing_name: string
        :param append_type: the type of characters to add to make it unique -
                            either 'number-sequence' or 'random-hex'.
        :type append_type: string
        '''
        ideal_name = ideal_name[:PACKAGE_NAME_MAX_LENGTH]
        if existing_name == ideal_name:
            return ideal_name
        if append_type == 'number-sequence':
            MAX_NUMBER_APPENDED = 999
            APPEND_MAX_CHARS = len(str(MAX_NUMBER_APPENDED))
        elif append_type == 'random-hex':
            APPEND_MAX_CHARS = 5  # 16^5 = 1 million combinations
        else:
            raise NotImplementedError('append_type cannot be %s' % append_type)
        # Find out which package names have been taken. Restrict it to names
        # derived from the ideal name plus and numbers added
        like_q = u'%s%%' % \
            ideal_name[:PACKAGE_NAME_MAX_LENGTH-APPEND_MAX_CHARS]
        name_results = Session.query(Package.name)\
                              .filter(Package.name.ilike(like_q))\
                              .all()
        taken = set([name_result[0] for name_result in name_results])
        if existing_name and existing_name in taken:
            taken.remove(existing_name)
        if ideal_name not in taken:
            # great, the ideal name is available
            return ideal_name
        elif existing_name and existing_name.startswith(ideal_name):
            # the ideal name is not available, but its an existing dataset with
            # a name based on the ideal one, so there's no point changing it to
            # a different number
            return existing_name
        elif append_type == 'number-sequence':
            # find the next available number
            counter = 1
            while counter <= MAX_NUMBER_APPENDED:
                candidate_name = \
                    ideal_name[:PACKAGE_NAME_MAX_LENGTH-len(str(counter))] + \
                    str(counter)
                if candidate_name not in taken:
                    return candidate_name
                counter = counter + 1
            return None
        elif append_type == 'random-hex':
            return ideal_name[:PACKAGE_NAME_MAX_LENGTH-APPEND_MAX_CHARS] + \
                str(uuid.uuid4())[:APPEND_MAX_CHARS]

    _save_gather_error = HarvestGatherError.create
    _save_object_error = HarvestObjectError.create

    def _get_user_name(self):
        '''
        Returns the name of the user that will perform the harvesting actions
        (deleting, updating and creating datasets)

        By default this will be the old 'harvest' user to maintain
        compatibility. If not present, the internal site admin user will be
        used. This is the recommended setting, but if necessary it can be
        overridden with the `ckanext.harvest.user_name` config option:

           ckanext.harvest.user_name = harvest

        '''
        if self._user_name:
            return self._user_name

        config_user_name = config.get('ckanext.harvest.user_name')
        if config_user_name:
            self._user_name = config_user_name
            return self._user_name

        context = {'model': model,
                   'ignore_auth': True,
                   }

        # Check if 'harvest' user exists and if is a sysadmin
        try:
            user_harvest = p.toolkit.get_action('user_show')(
                context, {'id': 'harvest'})
            if user_harvest['sysadmin']:
                self._user_name = 'harvest'
                return self._user_name
        except p.toolkit.ObjectNotFound:
            pass

        context['defer_commit'] = True  # See ckan/ckan#1714
        self._site_user = p.toolkit.get_action('get_site_user')(context, {})
        self._user_name = self._site_user['name']

        return self._user_name

    def _create_harvest_objects(self, remote_ids, harvest_job):
        '''
        Given a list of remote ids and a Harvest Job, create as many Harvest Objects and
        return a list of their ids to be passed to the fetch stage.

        TODO: Not sure it is worth keeping this function
        '''
        try:
            object_ids = []
            if len(remote_ids):
                for remote_id in remote_ids:
                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid = remote_id, job = harvest_job)
                    obj.save()
                    object_ids.append(obj.id)
                return object_ids
            else:
               self._save_gather_error('No remote datasets could be identified', harvest_job)
        except Exception, e:
            self._save_gather_error('%r' % e.message, harvest_job)

    @maintain.deprecated('Use the harvest_object.get_extra method instead.')
    def _get_object_extra(self, harvest_object, key):
        '''
        Deprecated!

        Helper function for retrieving the value from a harvest object extra,
        given the key
        '''
        return harvest_object.get_extra(key)

    def _create_or_update_package(self, package_dict, harvest_object):
        '''
        Creates a new package or updates an exisiting one according to the
        package dictionary provided. The package dictionary should look like
        the REST API response for a package:

        http://ckan.net/api/rest/package/statistics-catalunya

        Note that the package_dict must contain an id, which will be used to
        check if the package needs to be created or updated (use the remote
        dataset id).

        If the remote server provides the modification date of the remote
        package, add it to package_dict['metadata_modified'].

        :returns: The same as what import_stage should return. i.e. True if the
                  create or update occurred ok, 'unchanged' if it didn't need
                  updating or False if there were errors.

        TODO: Not sure it is worth keeping this function. If useful it should
        use the output of package_show logic function (maybe keeping support
        for rest api based dicts
        '''
        try:
            # Change default schema
            schema = default_create_package_schema()
            schema['id'] = [ignore_missing, unicode]
            schema['__junk'] = [ignore]

            # Check API version
            if self.config:
                try:
                    api_version = int(self.config.get('api_version', 2))
                except ValueError:
                    raise ValueError('api_version must be an integer')
            else:
                api_version = 2

            user_name = self._get_user_name()
            context = {
                'model': model,
                'session': Session,
                'user': user_name,
                'api_version': api_version,
                'schema': schema,
                'ignore_auth': True,
            }

            if self.config and self.config.get('clean_tags', False):
                tags = package_dict.get('tags', [])
                tags = [munge_tag(t) for t in tags if munge_tag(t) != '']
                tags = list(set(tags))
                package_dict['tags'] = tags

            # Check if package exists
            try:
                existing_package_dict = self._find_existing_package(package_dict)

                # In case name has been modified when first importing. See issue #101.
                package_dict['name'] = existing_package_dict['name']

                # Check modified date
                if not 'metadata_modified' in package_dict or \
                   package_dict['metadata_modified'] > existing_package_dict.get('metadata_modified'):
                    log.info('Package with GUID %s exists and needs to be updated' % harvest_object.guid)
                    # Update package
                    context.update({'id':package_dict['id']})
                    package_dict.setdefault('name',
                            existing_package_dict['name'])
                    new_package = get_action('package_update_rest')(context, package_dict)

                else:
                    log.info('Package with GUID %s not updated, skipping...' % harvest_object.guid)
                    # NB harvest_object.current/package_id are not set
                    return 'unchanged'

                # Flag the other objects linking to this package as not current anymore
                from ckanext.harvest.model import harvest_object_table
                conn = Session.connection()
                u = update(harvest_object_table) \
                        .where(harvest_object_table.c.package_id==bindparam('b_package_id')) \
                        .values(current=False)
                conn.execute(u, b_package_id=new_package['id'])

                # Flag this as the current harvest object

                harvest_object.package_id = new_package['id']
                harvest_object.current = True
                harvest_object.save()

            except NotFound:
                # Package needs to be created

                # Get rid of auth audit on the context otherwise we'll get an
                # exception
                context.pop('__auth_audit', None)

                # Set name for new package to prevent name conflict, see issue #117
                if package_dict.get('name', None):
                    package_dict['name'] = self._gen_new_name(package_dict['name'])
                else:
                    package_dict['name'] = self._gen_new_name(package_dict['title'])

                log.info('Package with GUID %s does not exist, let\'s create it' % harvest_object.guid)
                harvest_object.current = True
                harvest_object.package_id = package_dict['id']
                # Defer constraints and flush so the dataset can be indexed with
                # the harvest object id (on the after_show hook from the harvester
                # plugin)
                harvest_object.add()

                model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
                model.Session.flush()

                new_package = get_action('package_create_rest')(context, package_dict)

            Session.commit()
            return True

        except ValidationError,e:
            log.exception(e)
            self._save_object_error('Invalid package with GUID %s: %r'%(harvest_object.guid,e.error_dict),harvest_object,'Import')
        except Exception, e:
            log.exception(e)
            self._save_object_error('%r'%e,harvest_object,'Import')

        return None

    def _find_existing_package(self, package_dict):
        data_dict = {'id': package_dict['id']}
        package_show_context = {'model': model, 'session': Session,
                                'ignore_auth': True}
        return get_action('package_show')(
            package_show_context, data_dict)

    @classmethod
    def get_metadata_provenance_for_just_this_harvest(cls, harvest_object):
        return {
            'activity_occurred': datetime.datetime.utcnow().isoformat(),
            'activity': 'harvest',
            'harvest_source_url': harvest_object.source.url,
            'harvest_source_title': harvest_object.source.title,
            'harvest_source_type': harvest_object.source.type,
            'harvested_guid': harvest_object.guid,
            'harvested_metadata_modified': harvest_object.metadata_modified_date.isoformat()
                 if harvest_object.metadata_modified_date else None,
            }

    @classmethod
    def get_metadata_provenance(cls, harvest_object, harvested_provenance=None):
        '''Returns the metadata_provenance for a dataset, which is the details
        of this harvest added onto any existing metadata_provenance value in
        the dataset. This should be stored in the metadata_provenance extra
        when harvesting.

        Provenance is a record of harvests, imports and perhaps other
        activities of production too, as suggested by W3C PROV.

        This helps keep track when a dataset is created in site A, imported
        into site B, harvested into site C and from there is harvested into
        site D. The metadata_provence will be a list of four dicts with the
        details: [A, B, C, D].
        '''
        if isinstance(harvested_provenance, basestring):
            harvested_provenance = json.loads(harvested_provenance)
        elif harvested_provenance is None:
            harvested_provenance = []
        assert isinstance(harvested_provenance, list)
        metadata_provenance = harvested_provenance + \
            [cls.get_metadata_provenance_for_just_this_harvest(harvest_object)]
        return json.dumps(metadata_provenance)

    def _transfer_current(self, previous_object, harvest_object):
        '''Transfer "current" flag to this harvest_object to show it is was the
        last successful import.

        NB This should be called at the end of a successful import. The problem
        with doing it any earlier is that a harvest that gets skipped after
        this point will still be marked 'current'. This gives two problems:
        1. queue.py will set obj.report_status = 'updated' rather than
           'unchanged'.
        2. harvest_object_show with param dataset_id will show you the skipped
           object.
        '''
        if previous_object:
            previous_object.current = False
            harvest_object.package_id = previous_object.package_id
            previous_object.add()
        harvest_object.current = True
        model.Session.commit()
        model.Session.remove()

    @classmethod
    def _match_resources_with_existing_ones(cls, res_dicts,
                                            existing_resources):
        '''Adds IDs to given resource dicts, based on existing resources, so
        that when we do package_update the resources are updated rather than
        deleted and recreated.

        Edits the res_dicts in-place

        :param res_dicts: Resources to have the ID added
        :param existing_resources: Existing resources - have IDs. dicts or
                                   Resource objects
        :returns: None
        '''
        unmatched_res_dicts = [res_dict for res_dict in res_dicts
                               if 'id' not in res_dict]
        if not unmatched_res_dicts:
            log.info('Resource IDs exist alread (%s resources)',
                     len(res_dicts))
            return
        if existing_resources and isinstance(existing_resources[0], dict):
            unmatched_existing_res_dicts = existing_resources[:]
        else:
            unmatched_existing_res_dicts = [dict(id=res.id, url=res.url,
                                                 name=res.name,
                                                 description=res.description)
                                            for res in existing_resources]

        def find_matches(match_func):
            for res_dict in unmatched_res_dicts[:]:
                for existing_res_dict in unmatched_existing_res_dicts:
                    if match_func(res_dict, existing_res_dict):
                        res_dict['id'] = existing_res_dict['id']
                        unmatched_existing_res_dicts.remove(existing_res_dict)
                        unmatched_res_dicts.remove(res_dict)
                        break
        find_matches(lambda res1, res2: res1['url'] == res2['url'] and
                     res1.get('name') == res2['name'] and
                     res1.get('description') == res2['description'])
        find_matches(lambda res1, res2: res1['url'] == res2['url'])
        log.info('Matched resources to existing ones: %s/%s',
                 len(res_dicts)-len(unmatched_res_dicts), len(res_dicts))


