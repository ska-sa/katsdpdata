#!groovy

@Library('katsdpjenkins@python2') _

katsdp.killOldJobs()
katsdp.setDependencies(['ska-sa/katsdpdockerbase/python2',
                        'ska-sa/katdal/master', 'ska-sa/katpoint/master',
                        'ska-sa/katsdpservices/master',
                        'ska-sa/katsdptelstate/master'])
katsdp.standardBuild(katsdpdockerbase_ref: 'python2')
katsdp.mail('cschollar@ska.ac.za thomas@ska.ac.za')
