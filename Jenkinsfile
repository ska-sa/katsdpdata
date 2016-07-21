#!groovy

def katsdp = fileLoader.fromGit('jenkins/scripts/katsdp.groovy', 'git@github.com:ska-sa/katsdpinfrastructure', 'master', 'katpull', '')

katsdp.setDependencies(['ska-sa/katsdpdockerbase/master'])
katsdp.standardBuild(maintainer: 'bmerry@ska.ac.za', subdir: 'katsdpfilewriter')
