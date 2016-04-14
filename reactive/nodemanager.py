from charms.reactive import when, when_not, set_state, remove_state
from charms.layer.apache_bigtop_base import get_bigtop_base, get_layer_opts
from charmhelpers.core import host, hookenv


@when_not('resourcemanager.joined')
def blocked():
    hookenv.status_set('blocked', 'waiting for resourcemanager relation')


@when('resourcemanager.joined')
@when_not('nodemanager.installed')
def install_hadoop(resourcemanager):
    '''Install only if the resourcemanager has sent its FQDN.'''
    if resourcemanager.resourcemanagers():
        hookenv.status_set('maintenance', 'installing nodemanager')
        hostname = resourcemanager.resourcemanagers()[0]
        bigtop = get_bigtop_base()
        bigtop.install(NN=hostname)
        set_state('nodemanager.installed')
        hookenv.status_set('maintenance', 'nodemanager installed')
    else:
        hookenv.status_set('waiting', 'waiting for nodemanager to become ready')


@when('nodemanager.installed')
@when_not('nodemanager.started')
def start_nodemanager():
    hookenv.status_set('maintenance', 'starting nodemanager')
    host.service_start('hadoop-yarn-nodemanager')
    for port in get_layer_opts().exposed_ports('nodemanager'):
        hookenv.open_port(port)
    set_state('nodemanager.started')
    hookenv.status_set('active', 'ready')


@when('nodemanager.started')
@when_not('resourcemanager.joined')
def stop_nodemanager():
    hookenv.status_set('maintenance', 'stopping nodemanager')
    for port in get_layer_opts().exposed_ports('nodemanager'):
    host.service_stop('hadoop-yarn-nodemanager')
    remove_state('nodemanager.started')
    hookenv.status_set('maintenance', 'nodemanager stopped')
