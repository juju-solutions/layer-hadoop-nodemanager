# pylint: disable=unused-argument
from charms.reactive import is_state, when, when_not
from charmhelpers.core.hookenv import status_set


@when_not('apache-bigtop-nodemanager.started')
def prereq_status():
    hdfs_rel = is_state('namenode.joined')
    yarn_rel = is_state('resourcemanager.joined')
    yarn_ready = is_state('resourcemanager.ready')

    if not hdfs_rel:
        status_set('blocked', 'missing required namenode relation')
    elif not yarn_rel:
        status_set('blocked', 'missing required resourcemanager relation')
    elif yarn_rel and not yarn_ready:
        status_set('waiting', 'waiting for yarn to become ready')


@when('apache-bigtop-nodemanager.started')
def ready_status():
    status_set('active', 'ready')
