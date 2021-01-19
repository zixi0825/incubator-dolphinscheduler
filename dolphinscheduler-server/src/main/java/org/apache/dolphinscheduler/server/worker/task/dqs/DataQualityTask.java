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
package org.apache.dolphinscheduler.server.worker.task.dqs;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.process.Property;
import org.apache.dolphinscheduler.common.process.ResourceInfo;
import org.apache.dolphinscheduler.common.task.AbstractParameters;
import org.apache.dolphinscheduler.common.task.dqs.DataQualityParameters;
import org.apache.dolphinscheduler.common.task.dqs.rule.RuleDefinition;
import org.apache.dolphinscheduler.common.utils.CommonUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.ParameterUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.server.entity.TaskExecutionContext;
import org.apache.dolphinscheduler.server.utils.ParamUtils;
import org.apache.dolphinscheduler.server.utils.SparkArgsUtils;
import org.apache.dolphinscheduler.server.worker.task.AbstractYarnTask;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.RuleManager;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.parameter.DataQualityConfiguration;
import org.slf4j.Logger;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DataQualityTask
 */
public class DataQualityTask extends AbstractYarnTask {

    /**
     * spark2 command
     */
    private static final String SPARK2_COMMAND = "${SPARK_HOME2}/bin/spark2-submit";

    private DataQualityParameters dataQualityParameters;
    /**
     * taskExecutionContext
     */
    private final TaskExecutionContext taskExecutionContext;

    public DataQualityTask(TaskExecutionContext taskExecutionContext, Logger logger){
        super(taskExecutionContext, logger);
        this.taskExecutionContext = taskExecutionContext;
    }

    @Override
    public void init() throws Exception {
        logger.info(" data quality task params {}", taskExecutionContext.getTaskParams());

        dataQualityParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), DataQualityParameters.class);

        if (null == dataQualityParameters) {
            logger.error("data quality params is null");
            return;
        }

        if (!dataQualityParameters.checkParameters()) {
            throw new RuntimeException("data quality task params is not valid");
        }

        if(StringUtils.isEmpty(dataQualityParameters.getRuleJson())){
            throw new RuntimeException("rule json is null");
        }

        Map<String,String> inputParameter = dataQualityParameters.getRuleInputParameter();
        for(Map.Entry<String,String> entry: inputParameter.entrySet()){
            entry.setValue(entry.getValue().trim());
        }

        RuleDefinition ruleDefinition = JSONUtils.parseObject(dataQualityParameters.getRuleJson(),RuleDefinition.class);

        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime time = LocalDateTime.now();
        String now = df.format(time);

        inputParameter.put("rule_type",ruleDefinition.getRuleType().getCode()+"");
        inputParameter.put("create_time","'"+now+"'");
        inputParameter.put("update_time","'"+now+"'");
        inputParameter.put("threshold","1000");
        inputParameter.put("process_defined_id",taskExecutionContext.getTaskAppId().split("_")[0]);
        inputParameter.put("task_instance_id",taskExecutionContext.getTaskInstanceId()+"");
        inputParameter.put("check_type","1");

        RuleManager ruleManager = new RuleManager(
                ruleDefinition,
                inputParameter,
                taskExecutionContext.getDataQualityTaskExecutionContext());

        DataQualityConfiguration dataQualityConfiguration =
                ruleManager.generateDataQualityParameter();

        dataQualityParameters
                .getSparkParameters()
                .setMainArgs("\""+replaceDoubleBrackets(StringEscapeUtils.escapeJava(JSONUtils.toJsonString(dataQualityConfiguration)))+"\"");

        dataQualityParameters
                .getSparkParameters()
                .setQueue(taskExecutionContext.getQueue());

        setMainJarName();
    }

    @Override
    protected String buildCommand() throws Exception {
        List<String> args = new ArrayList<>();

        args.add(SPARK2_COMMAND);

        // other parameters
        args.addAll(SparkArgsUtils.buildArgs(dataQualityParameters.getSparkParameters()));

        // replace placeholder
        Map<String, Property> paramsMap = ParamUtils.convert(ParamUtils.getUserDefParamsMap(
                taskExecutionContext.getDefinedParams()),
                taskExecutionContext.getDefinedParams(),
                dataQualityParameters.getSparkParameters().getLocalParametersMap(),
                CommandType.of(taskExecutionContext.getCmdTypeIfComplement()),
                taskExecutionContext.getScheduleTime());

        String command = null;

        if (null != paramsMap) {
            command = ParameterUtils.convertParameterPlaceholders(String.join(" ", args), ParamUtils.convert(paramsMap));
        }

        logger.info("spark task command: {}", command);

        return command;
    }

    @Override
    protected void setMainJarName() {
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setRes(System.getProperty("user.dir") + File.separator + "lib" + File.separator + CommonUtils.getDqsJarName());
        dataQualityParameters.getSparkParameters().setMainJar(mainJar);
    }

    @Override
    public AbstractParameters getParameters() {
        return dataQualityParameters;
    }

    private String replaceDoubleBrackets(String mainParameter){
        mainParameter = mainParameter
                .replace(Constants.DOUBLE_BRACKETS_LEFT,Constants.DOUBLE_BRACKETS_LEFT_SPACE)
                .replace(Constants.DOUBLE_BRACKETS_RIGHT,Constants.DOUBLE_BRACKETS_RIGHT_SPACE);
        if(mainParameter.contains(Constants.DOUBLE_BRACKETS_LEFT) || mainParameter.contains(Constants.DOUBLE_BRACKETS_RIGHT)){
            return replaceDoubleBrackets(mainParameter);
        }else{
            return  mainParameter;
        }
    }

    public static void main(String[] args) {
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime time = LocalDateTime.now();
        String now = df.format(time);
        System.out.println("\""+now+"\"");
    }
}
