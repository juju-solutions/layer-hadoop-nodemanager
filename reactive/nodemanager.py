from charms.reactive import when, when_not, set_state, remove_state
from charms.layer.apache_bigtop_base import get_bigtop_base, get_layer_opts
from charmhelpers.core import host, hookenv


@when_not('resourcemanager.joined')
def blocked():
    hookenv.status_set('blocked', 'waiting for resourcemanager relation')


@when('resourcemanager.joined', 'puppet.available')
@when_not('nodemanager.installed')
def install_hadoop(resourcemanager):
    '''Install only if the resourcemanager has sent its FQDN.'''
    if resourcemanager.resourcemanagers():
        hookenv.status_set('maintenance', 'installing nodemanager')
        nn_host = resourcemanager.resourcemanagers()[0]
        rm_host = resourcemanager.resourcemanagers()[1]
        bigtop = get_bigtop_base()
        hosts = {'namenode': nn_host, 'resourcemanager': rm_host}
        bigtop.install(hosts=hosts, roles="['nodemanager', mapred-app']")
        set_state('nodemanager.installed')
        hookenv.status_set('maintenance', 'nodemanager installed')
    else:
        hookenv.status_set('waiting', 'waiting for resourcemanager to become ready')


@when('resourcemanager.joined')
@when('nodemanager.installed')
@when_not('nodemanager.started')
def start_nodemanager(resourcemanager):
    hookenv.status_set('maintenance', 'starting nodemanager')
    # NB: service should be started by install, but this may be handy in case
    # we have something that removes the .started state in the future. Also
    # note we restart here in case we modify conf between install and now.
    host.service_restart('hadoop-yarn-nodemanager')
    for port in get_layer_opts().exposed_ports('nodemanager'):
        hookenv.open_port(port)
    set_state('nodemanager.started')
    hookenv.status_set('active', 'ready')


@when('nodemanager.started')
@when_not('resourcemanager.joined')
def stop_nodemanager():
    hookenv.status_set('maintenance', 'stopping nodemanager')
    for port in get_layer_opts().exposed_ports('nodemanager'):
        hookenv.close_port(port)
    host.service_stop('hadoop-yarn-nodemanager')
    remove_state('nodemanager.started')
    # Remove the installed state so we can re-configure the installation
    # if/when a new resourcemanager comes along in the future.
    remove_state('nodemanager.installed')
    hookenv.status_set('maintenance', 'nodemanager stopped')
