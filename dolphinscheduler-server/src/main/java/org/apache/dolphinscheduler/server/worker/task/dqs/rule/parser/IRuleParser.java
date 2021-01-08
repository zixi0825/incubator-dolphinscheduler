/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.server.worker.task.dqs.rule.parser;

import org.apache.dolphinscheduler.server.entity.DataQualityTaskExecutionContext;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.RuleDefinition;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.parameter.DataQualityConfiguration;
import java.util.Map;

/**
 * IRuleParser
 */
public interface IRuleParser {

    DataQualityConfiguration parse(RuleDefinition ruleDefinition,
                                   Map<String,String> inputParameterValue,
                                   DataQualityTaskExecutionContext dataQualityTaskExecutionContext) throws Exception;
}
