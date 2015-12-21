
from ckan import logic
from ckan import model
import ckan.lib.helpers as h
import ckan.plugins as p

from ckanext.harvest.model import UPDATE_FREQUENCIES
from ckanext.harvest.interfaces import IHarvester

def harvesters_info():
    context = {'model': model, 'user': p.toolkit.c.user or p.toolkit.c.author}
    return logic.get_action('harvesters_info_show')(context,{})

def harvester_types():
    harvesters = harvesters_info()
    return [{'text': p.toolkit._(h['title']), 'value': h['name']}
            for h in harvesters]

def harvest_frequencies():

    return [{'text': p.toolkit._(f.title()), 'value': f}
            for f in UPDATE_FREQUENCIES]

def link_for_harvest_object(id=None, guid=None, text=None):

    if not id and not guid:
        return None

    if guid:
        context = {'model': model, 'user': p.toolkit.c.user or p.toolkit.c.author}
        obj =logic.get_action('harvest_object_show')(context, {'id': guid, 'attr': 'guid'})
        id = obj.id

    url = h.url_for('harvest_object_show', id=id)
    text = text or guid or id
    link = '<a href="{url}">{text}</a>'.format(url=url, text=text)

    return p.toolkit.literal(link)

def harvest_source_extra_fields():
    fields = {}
    for harvester in p.PluginImplementations(IHarvester):
        if not hasattr(harvester, 'extra_schema'):
            continue
        fields[harvester.info()['name']] = harvester.extra_schema().keys()
    return fields

def get_org_ids():
    return [o['id'] for o in h.organizations_available('create_dataset')] if p.toolkit.c.userobj else []

def get_active_sources(sources):
    return [s for s in sources if s['active']]

def allowed_sources(org_ids):
    return [s for s in p.toolkit.c.sources if s['publisher_id'] in org_ids]