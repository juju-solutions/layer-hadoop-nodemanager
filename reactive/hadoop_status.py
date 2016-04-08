# pylint: disable=unused-argument
from charms.reactive import when, when_not
from charmhelpers.core import hookenv


@when('hadoop.installed')
@when_not('resourcemanager.joined')
def blocked():
    hookenv.status_set('blocked', 'Waiting for relation to ResourceManager')


@when('hadoop.installed', 'resourcemanager.joined')
@when_not('resourcemanager.spec.mismatch', 'resourcemanager.ready')
def waiting(resourcemanager):
    hookenv.status_set('waiting', 'Waiting for ResourceManager')


@when('nodemanager.started')
def ready():
    hookenv.status_set('active', 'Ready')
