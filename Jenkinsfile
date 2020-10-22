#!groovy

@Library('katsdpjenkins') _

katsdp.killOldJobs()
katsdp.setDependencies(['ska-sa/katsdpdockerbase/master',
                        'ska-sa/katdal/master', 'ska-sa/katpoint/master',
                        'ska-sa/katsdpservices/master',
                        'ska-sa/katsdptelstate/master'])
katsdp.standardBuild(
    python3: true,
    python2: false)
katsdp.mail('thomas@ska.ac.za')
