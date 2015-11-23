import os
from logging import getLogger

import ckan.plugins as p
from ckanext.harvest.model import setup as model_setup


log = getLogger(__name__)
assert not log.disabled

class Harvest(p.SingletonPlugin):

    p.implements(p.IConfigurable)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.ITemplateHelpers)

    def configure(self, config):

        # Setup harvest model
        model_setup()

    def before_map(self, map):

        controller = 'ckanext.harvest.controllers.view:ViewController'
        map.redirect('/harvest/', '/harvest') # because there are relative links
        map.connect('harvest', '/harvest',controller=controller,action='index')

        map.connect('harvest_new', '/harvest/new', controller=controller, action='new')
        map.connect('harvest_edit', '/harvest/edit/:id', controller=controller, action='edit')
        map.connect('harvest_delete', '/harvest/delete/:id',controller=controller, action='delete')
        map.connect('harvest_source', '/harvest/:id', controller=controller, action='read')

        map.connect('harvesting_job_create', '/harvest/refresh/:id',controller=controller,
                action='create_harvesting_job')

        map.connect('harvest_object_show', '/harvest/object/:id', controller=controller, action='show_object')

        return map

    def update_config(self, config):
        # check if new templates
        templates = 'templates'
        # DGU Hack - stick to genshi for now
        #if p.toolkit.check_ckan_version(min_version='2.0'):
        #    if not p.toolkit.asbool(config.get('ckan.legacy_templates', False)):
        #        templates = 'templates_new'
        p.toolkit.add_template_directory(config, templates)
        p.toolkit.add_public_directory(config, 'public')

    ## IActions

    def get_actions(self):

        module_root = 'ckanext.harvest.logic.action'
        action_functions = _get_logic_functions(module_root)

        return action_functions

    ## IAuthFunctions

    def get_auth_functions(self):

        module_root = 'ckanext.harvest.logic.auth'
        auth_functions = _get_logic_functions(module_root)

        return auth_functions

    ## ITemplateHelpers

    def get_helpers(self):
        from ckanext.harvest import helpers as harvest_helpers
        return {
                #'package_list_for_source': harvest_helpers.package_list_for_source,
                'harvesters_info': harvest_helpers.harvesters_info,
                'harvester_types': harvest_helpers.harvester_types,
                'harvest_frequencies': harvest_helpers.harvest_frequencies,
                'link_for_harvest_object': harvest_helpers.link_for_harvest_object,
                'harvest_source_extra_fields': harvest_helpers.harvest_source_extra_fields,
                }

def _get_logic_functions(module_root, logic_functions={}):

    for module_name in ['get', 'create', 'update', 'delete']:
        module_path = '%s.%s' % (module_root, module_name,)
        module = __import__(module_path)

        for part in module_path.split('.')[1:]:
            module = getattr(module, part)

        for key, value in module.__dict__.items():
            if not key.startswith('_') and  (hasattr(value, '__call__')
                        and (value.__module__ == module_path)):
                logic_functions[key] = value

    return logic_functions
