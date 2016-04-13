from charms.reactive import when, when_not, set_state, remove_state
from charms.layer.apache_bigtop_base import get_bigtop_base
from jujubigdata.handlers import YARN
from jujubigdata import utils


@when('resourcemanager.ready')
@when_not('nodemanager.started')
def start_nodemanager(resourcemanager):
    bigtop = get_bigtop_base()
    yarn = YARN(bigtop)
    utils.install_ssh_key('yarn', resourcemanager.ssh_key())
    utils.update_kv_hosts(resourcemanager.hosts_map())
    utils.manage_etc_hosts()
    # TODO add direct invocation to the init.d script
    bigtop.open_ports('nodemanager')
    set_state('nodemanager.started')


@when('nodemanager.started')
@when_not('resourcemanager.ready')
def stop_nodemanager():
    bigtop = get_bigtop_base()
    # TODO add direct invocation to the init.d script
    bigtop.close_ports('nodemanager')
    remove_state('nodemanager.started')
