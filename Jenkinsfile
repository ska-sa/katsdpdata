#!groovy

@Library('katsdpjenkins') _

katsdp.setDependencies(['ska-sa/katsdpdockerbase/master'])
katsdp.standardBuild(subdir: 'psrchive', docker_timeout: [time: 120, unit: 'MINUTES'])
katsdp.standardBuild(subdir: 'katsdpfilewriter')
katsdp.standardBuild()
katsdp.mail('cschollar@ska.ac.za bmerry@ska.ac.za thomas@ska.ac.za')
