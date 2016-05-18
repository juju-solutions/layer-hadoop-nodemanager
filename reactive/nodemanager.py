from charms.reactive import when, when_not, set_state, remove_state, is_state
from charms.reactive.helpers import data_changed
from charms.layer.apache_bigtop_base import Bigtop, get_layer_opts
from charmhelpers.core import host, hookenv


@when('bigtop.available', 'namenode.joined', 'resourcemanager.joined')
def install_nodemanager(namenode, resourcemanager):
    """Install if we have FQDNs.

    We only need the master FQDNs to perform the nodemanager install, so poll
    for master host data from the appropriate relation. This allows us to
    install asap, even if '<master>.ready' is not set.
    """
    namenodes = namenode.namenodes()
    resourcemanagers = resourcemanager.resourcemanagers()
    masters = namenodes + resourcemanagers
    if namenodes and resourcemanagers and data_changed('nm.masters', masters):
        installed = is_state('apache-bigtop-datanode.installed')
        action = 'installing' if not installed else 'configuring'
        hookenv.status_set('maintenance', '%s nodemanager' % action)
        bigtop = Bigtop()
        bigtop.render_site_yaml(
            hosts={
                'namenode': namenodes[0],
                'resourcemanager': resourcemanagers[0],
            },
            roles=[
                'nodemanager',
                'mapred-app',
            ],
        )
        bigtop.queue_puppet()
        set_state('apache-bigtop-nodemanager.pending')


@when('apache-bigtop-nodemanager.pending')
@when_not('apache-bigtop-base.puppet_queued')
def finish_install_nodemanager():
    remove_state('apache-bigtop-nodemanager.pending')
    set_state('apache-bigtop-nodemanager.installed')
    installed = is_state('apache-bigtop-datanode.installed')
    action = 'installed' if not installed else 'configured'
    hookenv.status_set('maintenance', 'nodemanager %s' % action)


@when('apache-bigtop-nodemanager.installed')
@when('namenode.joined', 'resourcemanager.joined')
@when_not('namenode.ready', 'resourcemanager.ready')
def send_nm_spec(namenode, resourcemanager):
    """Send our nodemanager spec so the master relations can become ready."""
    bigtop = Bigtop()
    namenode.set_local_spec(bigtop.spec())
    resourcemanager.set_local_spec(bigtop.spec())


@when('apache-bigtop-nodemanager.installed')
@when('namenode.ready', 'resourcemanager.ready')
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
    hookenv.status_set('maintenance', 'nodemanager stopped')
