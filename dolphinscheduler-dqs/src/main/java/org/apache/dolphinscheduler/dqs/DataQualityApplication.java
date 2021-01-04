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
package org.apache.dolphinscheduler.dqs;

import org.apache.dolphinscheduler.dqs.configuration.DataQualityConfiguration;
import org.apache.dolphinscheduler.dqs.context.DataQualityContext;
import org.apache.dolphinscheduler.dqs.flow.DataQualityTask;
import org.apache.dolphinscheduler.dqs.flow.connector.ConnectorFactory;
import org.apache.dolphinscheduler.dqs.flow.executor.SparkSqlExecuteTask;
import org.apache.dolphinscheduler.dqs.flow.writer.WriterFactory;
import org.apache.dolphinscheduler.dqs.utils.JSONUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DataQualityApplication
 */
public class DataQualityApplication {

    private static final Logger logger = LoggerFactory.getLogger(DataQualityApplication.class);

    public static void main(String[] args) throws Exception {
        logger.info(JSONUtil.toJson(args));

        if (args.length < 1) {
            logger.error("Usage: class <env-param> <dq-param>");
            System.exit(-1);
        }

        String dataQualityParameter = args[0];
        logger.info("dataQualityParameter: "+dataQualityParameter);

        DataQualityConfiguration dataQualityConfiguration = JSONUtil.fromJson(dataQualityParameter,DataQualityConfiguration.class);
        if(dataQualityConfiguration == null){
            System.exit(-1);
        }

        SparkConf conf = new SparkConf().setAppName(dataQualityConfiguration.getName());
        for(Map.Entry<String,String> entry: dataQualityConfiguration.getSparkParameter().getConfig().entrySet()){
            conf.set(entry.getKey(), entry.getValue());
        }

        conf.set("spark.sql.crossJoin.enabled", "true");
//        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        DataQualityContext context = new DataQualityContext(
                sparkSession,
                dataQualityConfiguration.getConnectorParameters(),
                dataQualityConfiguration.getExecutorParameters(),
                dataQualityConfiguration.getWriterParams());

        execute(buildDataQualityFlow(context));
        sparkSession.stop();
    }

    private static List<DataQualityTask> buildDataQualityFlow(DataQualityContext context) throws Exception{
        List<DataQualityTask> taskList =
                new ArrayList<>(ConnectorFactory.getInstance().getConnectors(context));
        taskList.add(new SparkSqlExecuteTask(context.getSparkSession(),context.getExecutorParameterList()));
        taskList.addAll(WriterFactory.getInstance().getWriters(context));

        return taskList;
    }

    private static void execute(List<DataQualityTask> taskList){
        for(DataQualityTask task: taskList){
            task.execute();
        }
    }
}
