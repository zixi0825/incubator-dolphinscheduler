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
package org.apache.dolphinscheduler.common.task.dqs;

import org.apache.commons.collections.MapUtils;
import org.apache.dolphinscheduler.common.enums.FormType;
import org.apache.dolphinscheduler.common.enums.ValueType;
import org.apache.dolphinscheduler.common.process.ResourceInfo;
import org.apache.dolphinscheduler.common.task.AbstractParameters;
import org.apache.dolphinscheduler.common.task.dqs.rule.ComparisonParameter;
import org.apache.dolphinscheduler.common.task.dqs.rule.RuleDefinition;
import org.apache.dolphinscheduler.common.task.dqs.rule.RuleInputEntry;
import org.apache.dolphinscheduler.common.task.spark.SparkParameters;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataQualityParameters
 */
public class DataQualityParameters extends AbstractParameters {

    private static  final Logger logger = LoggerFactory.getLogger(DataQualityParameters.class);

    private int ruleId;
    private String ruleJson;
    private Map<String,String> ruleInputParameter;
    private SparkParameters sparkParameters;

    public int getRuleId() {
        return ruleId;
    }

    public void setRuleId(int ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleJson() {
        return ruleJson;
    }

    public void setRuleJson(String ruleJson) {
        this.ruleJson = ruleJson;
    }

    public Map<String, String> getRuleInputParameter() {
        return ruleInputParameter;
    }

    public void setRuleInputParameter(Map<String, String> ruleInputParameter) {
        this.ruleInputParameter = ruleInputParameter;
    }

    /**
     * In this function ,we need more detailed check every parameter,
     * if the parameter is non-conformant will return false
     * @return boolean result
     */
    @Override
    public boolean checkParameters() {

        if(ruleId == 0){
            return false;
        }

        if(StringUtils.isEmpty(ruleJson)){
            return false;
        }

        RuleDefinition ruleDefinition = JSONUtils.parseObject(ruleJson,RuleDefinition.class);
        if(ruleDefinition == null){
            return false;
        }

        if(MapUtils.isEmpty(ruleInputParameter)){
            return false;
        }

        List<RuleInputEntry> defaultInputEntryList  = ruleDefinition.getRuleInputEntryList();
        ComparisonParameter comparisonParameter = ruleDefinition.getComparisonParameter();
        defaultInputEntryList.addAll(comparisonParameter.getInputEntryList());

        for(RuleInputEntry ruleInputEntry: defaultInputEntryList){
            System.out.println(JSONUtils.toJsonString(ruleInputEntry));
            if(ruleInputEntry.getCanEdit() && FormType.INPUT == ruleInputEntry.getType()){
                System.out.println("1: "+JSONUtils.toJsonString(ruleInputEntry));
                String value = ruleInputParameter.get(ruleInputEntry.getField());
                if(StringUtils.isNotEmpty(value)){
                    System.out.println("2: "+value);
                    ValueType valueType = ruleInputEntry.getValueType();
                    switch (valueType){
                        case STRING:
                            if(value.contains(",")){
                                logger.error(ruleInputEntry.getField() +" can not contains , ");
                                return false;
                            }
                            break;
                        case NUMBER:
                            if(!isNum(value)){
                                logger.error(ruleInputEntry.getField() +" should be a num ");
                                return false;
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        return sparkParameters != null;
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return null;
    }

    public SparkParameters getSparkParameters() {
        return sparkParameters;
    }

    public void setSparkParameters(SparkParameters sparkParameters) {
        this.sparkParameters = sparkParameters;
    }

    private boolean isNum(String str){
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        return isNum.matches();
    }
}
