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
package org.apache.dolphinscheduler.server.utils;

import org.apache.dolphinscheduler.common.enums.DbType;
import org.apache.dolphinscheduler.common.task.dqs.rule.*;
import org.apache.dolphinscheduler.common.utils.ParameterUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.dao.datasource.BaseDataSource;
import org.apache.dolphinscheduler.dao.datasource.DataSourceFactory;
import org.apache.dolphinscheduler.server.entity.DataQualityTaskExecutionContext;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.parameter.ConnectorParameter;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.parameter.ExecutorParameter;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.parameter.WriterParameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.dolphinscheduler.common.Constants.*;
import static org.apache.dolphinscheduler.common.Constants.DRIVER;

/**
 * RuleParserUtils
 */
public class RuleParserUtils {

    public static List<ConnectorParameter> getConnectorParameterList(
                                            Map<String, String> inputParameterValue,
                                            DataQualityTaskExecutionContext dataQualityTaskExecutionContext) throws Exception{

        List<ConnectorParameter> connectorParameterList = new ArrayList<>();

        if(StringUtils.isNotEmpty(dataQualityTaskExecutionContext.getSourceConnectorType())){
            BaseDataSource baseDataSource = DataSourceFactory.getDatasource(DbType.of(dataQualityTaskExecutionContext.getSourceType()),
                    dataQualityTaskExecutionContext.getSourceConnectionParams());
            ConnectorParameter sourceConnectorParameter = new ConnectorParameter();
            sourceConnectorParameter.setType(dataQualityTaskExecutionContext.getSourceConnectorType());
            Map<String,Object> config = new HashMap<>();
            if(baseDataSource != null){
                config.put(DATABASE,baseDataSource.getDatabase());
                config.put(TABLE,inputParameterValue.get(SRC_TABLE));
                config.put(URL,baseDataSource.getJdbcUrl());
                config.put(USER,baseDataSource.getUser());
                config.put(PASSWORD,baseDataSource.getPassword());
                config.put(DRIVER,DataSourceFactory.getDriver(DbType.of(dataQualityTaskExecutionContext.getSourceType())));
            }
            sourceConnectorParameter.setConfig(config);

            connectorParameterList.add(sourceConnectorParameter);
        }

        if(StringUtils.isNotEmpty(dataQualityTaskExecutionContext.getTargetConnectorType())){
            BaseDataSource targetDataSource = DataSourceFactory.getDatasource(DbType.of(dataQualityTaskExecutionContext.getTargetType()),
                    dataQualityTaskExecutionContext.getSourceConnectionParams());
            ConnectorParameter targetConnectorParameter = new ConnectorParameter();
            targetConnectorParameter.setType(dataQualityTaskExecutionContext.getTargetConnectorType());
            Map<String,Object> config = new HashMap<>();
            if(targetDataSource != null){
                config.put(DATABASE,targetDataSource.getDatabase());
                config.put(TABLE,inputParameterValue.get(TARGET_TABLE));
                config.put(URL,targetDataSource.getJdbcUrl());
                config.put(USER,targetDataSource.getUser());
                config.put(PASSWORD,targetDataSource.getPassword());
                config.put(DRIVER,DataSourceFactory.getDriver(DbType.of(dataQualityTaskExecutionContext.getTargetType())));
            }
            targetConnectorParameter.setConfig(config);

            connectorParameterList.add(targetConnectorParameter);
        }

        return connectorParameterList;
    }

    public static  int replaceExecuteSqlPlaceholder(RuleDefinition ruleDefinition,
                                             int index, Map<String, String> inputParameterValueResult,
                                             List<ExecutorParameter> executorParameterList) {
        List<ExecuteSqlDefinition> midExecuteSqlDefinitionList
                = ruleDefinition.getMidExecuteSqlList();

        List<ExecuteSqlDefinition> statisticsExecuteSqlDefinitionList
                = ruleDefinition.getStatisticsExecuteSqlList();

        if(StringUtils.isEmpty(inputParameterValueResult.get(SRC_FILTER))){
            if(midExecuteSqlDefinitionList != null){
                for(ExecuteSqlDefinition executeSqlDefinition:midExecuteSqlDefinitionList){
                    String sql = executeSqlDefinition.getSql();
                    sql = sql.replace("AND (${src_filter})","").replace("WHERE (${src_filter})","");
                    executeSqlDefinition.setSql(sql);
                }
            }

            for(ExecuteSqlDefinition executeSqlDefinition:statisticsExecuteSqlDefinitionList){
                String sql = executeSqlDefinition.getSql();
                sql = sql.replace("AND (${src_filter})","").replace("WHERE (${src_filter})","");
                executeSqlDefinition.setSql(sql);
            }
        }

        if(StringUtils.isEmpty(inputParameterValueResult.get(TARGET_FILTER))){
            if(midExecuteSqlDefinitionList != null){
                for(ExecuteSqlDefinition executeSqlDefinition:midExecuteSqlDefinitionList){
                    String sql = executeSqlDefinition.getSql();
                    sql = sql.replace("AND (${target_filter})","").replace("WHERE (${target_filter})","");
                    executeSqlDefinition.setSql(sql);
                }
            }

            for(ExecuteSqlDefinition executeSqlDefinition:statisticsExecuteSqlDefinitionList){
                String sql = executeSqlDefinition.getSql();
                sql = sql.replace("AND (${target_filter})","").replace("WHERE (${target_filter})","");
                executeSqlDefinition.setSql(sql);
            }
        }

        if(midExecuteSqlDefinitionList != null){
            for(ExecuteSqlDefinition executeSqlDefinition:midExecuteSqlDefinitionList){
                index = setExecutorParameter(
                        index,
                        inputParameterValueResult,
                        executorParameterList,
                        executeSqlDefinition);
            }
        }

        for(ExecuteSqlDefinition executeSqlDefinition:statisticsExecuteSqlDefinitionList){
            index = setExecutorParameter(
                    index,
                    inputParameterValueResult,
                    executorParameterList,
                    executeSqlDefinition);
        }

        return index;
    }

    private static int setExecutorParameter(int index,
                                     Map<String, String> inputParameterValueResult,
                                     List<ExecutorParameter> executorParameterList,
                                     ExecuteSqlDefinition executeSqlDefinition) {
        ExecutorParameter executorParameter = new ExecutorParameter();
        executorParameter.setIndex(index++ + "");
        executorParameter.setExecuteSql(ParameterUtils.convertParameterPlaceholders(executeSqlDefinition.getSql(),inputParameterValueResult));
        executorParameter.setTableAlias(executeSqlDefinition.getTableAlias());
        executorParameterList.add(executorParameter);
        return index;
    }

    private static String getCoalesceString(String table, String column){
        return "coalesce("+table+"."+column+", '')";
    }

    private static String getSrcColumnIsNullStr(String table,String[] columns){
        String[] columnList = new String[columns.length];
        for(int i=0; i<columns.length;i++){
            String column = columns[i];
            columnList[i] = table + "." + column + " IS NULL";
        }
        return  String.join(AND ,columnList);
    }

    public static Map<String,String> getInputParameterMapFromEntryList(RuleDefinition ruleDefinition){
        List<RuleInputEntry> defaultInputEntryList = ruleDefinition.getRuleInputEntryList();

        ComparisonParameter comparisonParameter = ruleDefinition.getComparisonParameter();
        if(comparisonParameter != null){
            defaultInputEntryList.addAll(comparisonParameter.getInputEntryList());
        }

        Map<String,String> defaultInputParameterValue = new HashMap<>();
        for(RuleInputEntry inputEntry:defaultInputEntryList){
            defaultInputParameterValue.put(inputEntry.getField(),inputEntry.getValue());
        }

        return defaultInputParameterValue;
    }

    public static List<WriterParameter> getWriterParameterList(
            String sql,
            DataQualityTaskExecutionContext dataQualityTaskExecutionContext) throws Exception{

        List<WriterParameter> writerParameterList = new ArrayList<>();

        if(StringUtils.isNotEmpty(dataQualityTaskExecutionContext.getWriterConnectorType())){
            BaseDataSource writerDataSource = DataSourceFactory.getDatasource(DbType.of(dataQualityTaskExecutionContext.getWriterType()),
                    dataQualityTaskExecutionContext.getWriterConnectionParams());
            WriterParameter writerParameter = new WriterParameter();
            writerParameter.setType(dataQualityTaskExecutionContext.getWriterConnectorType());
            Map<String,Object> config = new HashMap<>();
            if(writerDataSource != null){
                config.put(DATABASE,writerDataSource.getDatabase());
                config.put(TABLE,dataQualityTaskExecutionContext.getWriterTable());
                config.put(URL,writerDataSource.getJdbcUrl());
                config.put(USER,writerDataSource.getUser());
                config.put(PASSWORD,writerDataSource.getPassword());
                config.put(DRIVER,DataSourceFactory.getDriver(DbType.of(dataQualityTaskExecutionContext.getWriterType())));
                config.put(SQL,sql);
            }
            writerParameter.setConfig(config);

            writerParameterList.add(writerParameter);
        }

        return writerParameterList;

    }

    public static String getOnClause(String[] mappingColumnList,Map<String,String> inputParameterValueResult){
        //get on clause
        String[] columnList = new String[mappingColumnList.length];
        for(int i=0; i<mappingColumnList.length;i++){
            String column = mappingColumnList[i];
            columnList[i] = getCoalesceString(inputParameterValueResult.get(SRC_TABLE),column)
                    +" = "
                    + getCoalesceString(inputParameterValueResult.get(TARGET_TABLE),column);
        }

        return String.join(AND,columnList);
    }

    public static String getWhereClause(String[] mappingColumnList,Map<String,String> inputParameterValueResult){
        String srcColumnNotNull = "( NOT (" + getSrcColumnIsNullStr(inputParameterValueResult.get(SRC_TABLE),mappingColumnList) + " ))";
        String targetColumnIsNull = "( " + getSrcColumnIsNullStr(inputParameterValueResult.get(TARGET_TABLE),mappingColumnList) + " )";

        return srcColumnNotNull + AND + targetColumnIsNull;
    }

    public static List<WriterParameter> getWriterParameterList(
                                                  RuleDefinition ruleDefinition,
                                                  int index,
                                                  Map<String, String> inputParameterValueResult,
                                                  List<ExecutorParameter> executorParameterList,
                                                  DataQualityTaskExecutionContext dataQualityTaskExecutionContext,
                                                  String writerSql) throws Exception {
        ComparisonParameter comparisonParameter = ruleDefinition.getComparisonParameter();

        List<ExecuteSqlDefinition>  comparisonExecuteSqlList =
                comparisonParameter.getComparisonExecuteSqlList();

        ExecuteSqlDefinition comparisonSql = comparisonExecuteSqlList.get(0);
        inputParameterValueResult.put(COMPARISON_TABLE,comparisonSql.getTableAlias());

        if(StringUtils.isEmpty(inputParameterValueResult.get(SRC_FILTER))){
            for(ExecuteSqlDefinition executeSqlDefinition:comparisonExecuteSqlList){
                String sql = executeSqlDefinition.getSql();
                sql = sql.replace("AND (${src_filter})","").replace("WHERE (${src_filter})","");
                executeSqlDefinition.setSql(sql);
            }
        }

        if(StringUtils.isEmpty(inputParameterValueResult.get(TARGET_FILTER))){
            for(ExecuteSqlDefinition executeSqlDefinition:comparisonExecuteSqlList){
                String sql = executeSqlDefinition.getSql();
                sql = sql.replace("AND (${target_filter})","").replace("WHERE (${target_filter})","");
                executeSqlDefinition.setSql(sql);
            }
        }

        for(ExecuteSqlDefinition executeSqlDefinition:comparisonExecuteSqlList){
            index = setExecutorParameter(
                    index,
                    inputParameterValueResult,
                    executorParameterList,
                    executeSqlDefinition);
        }

        return getWriterParameterList(
                ParameterUtils.convertParameterPlaceholders(writerSql,inputParameterValueResult),
                dataQualityTaskExecutionContext
                );
    }
}
