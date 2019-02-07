#!groovy

@Library('katsdpjenkins') _

katsdp.setDependencies(['ska-sa/katsdpdockerbase/master',
                        'ska-sa/katdal/master', 'ska-sa/katpoint/master',
                        'ska-sa/katsdpservices/master',
                        'ska-sa/katsdptelstate/master'])
katsdp.standardBuild(subdir: 'katsdpmetawriter', python3: true, python2: false)
katsdp.standardBuild(subdir: 'katsdpdatawriter', python3: true, python2: false)
// Disabled for now because it does not build (SR-1606)
// katsdp.standardBuild(subdir: 'psrchive', docker_timeout: [time: 120, unit: 'MINUTES'])
katsdp.standardBuild()
katsdp.mail('cschollar@ska.ac.za bmerry@ska.ac.za thomas@ska.ac.za')
