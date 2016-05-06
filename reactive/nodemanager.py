from charms.reactive import when, when_not, set_state, remove_state
from charms.layer.apache_bigtop_base import get_bigtop_base, get_layer_opts
from charmhelpers.core import host, hookenv


@when('puppet.available', 'namenode.joined', 'resourcemanager.joined')
@when_not('apache-bigtop-nodemanager.installed')
def install_nodemanager(namenode, resourcemanager):
    """Install if we have FQDNs.

    We only need the master FQDNs to perform the nodemanager install, so poll
    for master host data from the appropriate relation. This allows us to
    install asap, even if '<master>.ready' is not set.
    """
    if namenode.namenodes() and resourcemanager.resourcemanagers():
        hookenv.status_set('maintenance', 'installing nodemanager')
        nn_host = namenode.namenodes()[0]
        rm_host = resourcemanager.resourcemanagers()[0]
        bigtop = get_bigtop_base()
        hosts = {'namenode': nn_host, 'resourcemanager': rm_host}
        bigtop.install(hosts=hosts, roles="['nodemanager', mapred-app']")
        set_state('apache-bigtop-nodemanager.installed')
        hookenv.status_set('maintenance', 'nodemanager installed')


@when('apache-bigtop-nodemanager.installed', 'namenode.joined', 'resourcemanager.joined')
@when_not('namenode.ready', 'resourcemanager.ready')
def send_nm_spec(namenode, resourcemanager):
    """Send our nodemanager spec so the master relations can become ready."""
    bigtop = get_bigtop_base()
    namenode.set_local_spec(bigtop.spec())
    resourcemanager.set_local_spec(bigtop.spec())


@when('apache-bigtop-nodemanager.installed', 'namenode.ready', 'resourcemanager.ready')
@when_not('apache-bigtop-nodemanager.started')
def start_nodemanager(namenode, resourcemanager):
    hookenv.status_set('maintenance', 'starting nodemanager')
    # NB: service should be started by install, but this may be handy in case
    # we have something that removes the .started state in the future. Also
    # note we restart here in case we modify conf between install and now.
    host.service_restart('hadoop-yarn-nodemanager')
    for port in get_layer_opts().exposed_ports('nodemanager'):
        hookenv.open_port(port)
    set_state('apache-bigtop-nodemanager.started')
    hookenv.status_set('maintenance', 'nodemanager started')


@when('apache-bigtop-nodemanager.started')
@when_not('resourcemanager.ready')
def stop_nodemanager():
    hookenv.status_set('maintenance', 'stopping nodemanager')
    for port in get_layer_opts().exposed_ports('nodemanager'):
        hookenv.close_port(port)
    host.service_stop('hadoop-yarn-nodemanager')
    remove_state('apache-bigtop-nodemanager.started')
    # Remove the installed state so we can re-configure the installation
    # if/when new masters come along in the future.
    remove_state('apache-bigtop-nodemanager.installed')
    hookenv.status_set('maintenance', 'nodemanager stopped')
