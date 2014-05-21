#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE.txt file distributed with
# this work for additional information regarding copyright ownership.  The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.    


java \
        -Djava.ext.dirs=/usr/local/oodt/cas-workflow/lib \
        -Djava.util.logging.config.file=/usr/local/oodt/conf/cas-workflow/etc/logging.properties \
        -Dorg.apache.oodt.cas.cli.action.spring.config=//usr/local/oodt/conf/cas-workflow/policy/cmd-line-actions.xml \
        -Dorg.apache.oodt.cas.cli.option.spring.config=//usr/local/oodt/conf/cas-workflow/policy/cmd-line-options.xml \
        org.apache.oodt.cas.workflow.system.XmlRpcWorkflowManagerClient $*
