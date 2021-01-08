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
package org.apache.dolphinscheduler.server.worker.task.dqs.rule;

import org.apache.dolphinscheduler.common.enums.*;
import org.apache.dolphinscheduler.server.entity.DataQualityTaskExecutionContext;
import org.apache.dolphinscheduler.server.utils.RuleParserUtils;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.parameter.*;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.placeholder.BusinessTimeUtils;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.parser.IRuleParser;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.parser.MultiTableAccuracyRuleParser;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.parser.MultiTableComparisonRuleParser;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.parser.SingleTableRuleParser;
import java.util.*;
import static org.apache.dolphinscheduler.common.Constants.*;

/**
 * RuleManager
 */
public class RuleManager {

    private String ruleJson;
    private Map<String,String> inputParameterValue;
    private DataQualityTaskExecutionContext dataQualityTaskExecutionContext;

    public static final String  CALCULATE_COMPARISON_WRITER_SQL = "SELECT ${rule_type} as rule_type," +
            "${task_id} as task_id," +
            "${task_instance_id} as task_instance_id," +
            "${statistics_name} AS statistics_value, " +
            "${comparison_name} AS comparison_value," +
            "${check_type} as check_type," +
            "${threshold} as threshold, " +
            "${create_time} as create_time," +
            "${update_time} as update_time " +
            "from ${statistics_table} FULL JOIN ${comparison_table}";

    public static final String  FIXED_COMPARISON_WRITER_SQL = "SELECT ${rule_type} as rule_type," +
            "${task_id} as task_id," +
            "${task_instance_id} as task_instance_id," +
            "${statistics_name} AS statistics_value, " +
            "${comparison_name} AS comparison_value," +
            "${check_type} as check_type," +
            "${threshold} as threshold, " +
            "${create_time} as create_time," +
            "${update_time} as update_time " +
            "from ${statistics_table}";

    public static final String MULTI_TABLE_COMPARISON_SQL = "SELECT ${rule_type} as rule_type," +
            "${task_id} as task_id," +
            "${task_instance_id} as task_instance_id," +
            "${statistics_name} AS statistics_value, " +
            "${comparison_name} AS comparison_value," +
            "${check_type} as check_type," +
            "${threshold} as threshold, " +
            "${create_time} as create_time," +
            "${update_time} as update_time " +
            "from ( ${statistics_execute_sql} )tmp1 "+
            "join "+
            "( ${comparison_execute_sql} ) tmp2 ";

    public RuleManager(String ruleJson, Map<String,String> inputParameterValue, DataQualityTaskExecutionContext dataQualityTaskExecutionContext){
        this.ruleJson = ruleJson;
        this.inputParameterValue = inputParameterValue;
        this.dataQualityTaskExecutionContext = dataQualityTaskExecutionContext;
    }

    /**
     * 设计的时候应该考虑设计模式，不同的类型采用不同的生成策略，不要采用一条龙的方式，一个方法写完
     * 抽取出共性的方法
     * @return
     * @throws Exception
     */
    public DataQualityConfiguration generateDataQualityParameter() throws Exception{

        //1.将ruleJson转成RuleDefinition
        //2.依次将RuleDefinition中${key}全部替换为inputParameterValue中的值
        //3
        /**
         * 拿到InputEntryList：包括常规的InputEntry和ComparisonEntry
         *  1. 拿到数据源类型的InputEntry，拿到datasource_id，然后获取连接信息进行封装成connectors
         *  2. 拿到 ExecuteSqlDefinitionList，然后替换，构造ExecutorParameterList
         *  3. 判断Comparison类型，然后生成相应的writer sql
         *  - 如果是计算类型的，那么将executesqldefinition 替换占位符，构造ExecutorParameter
         *  - 如果固定值类型，则不用处理
         *
         */
        //根据ruleType
        RuleDefinition ruleDefinition = JSONUtils.parseObject(ruleJson,RuleDefinition.class);

        if(ruleDefinition == null){
            return null;
        }

        //先根据InputEntryList获取一个Map，然后用inputParameterValue去替换里面的值
        Map<String,String> inputParameterValueResult = RuleParserUtils.getInputParameterMapFromEntryList(ruleDefinition);
        inputParameterValueResult.putAll(inputParameterValue);

        //对参数中的时间格式进行转换
        inputParameterValueResult.putAll(BusinessTimeUtils.getBusinessTime(CommandType.START_PROCESS,new Date()));

        IRuleParser ruleParser = null;
        switch (ruleDefinition.getRuleType()){
            case SINGLE_TABLE:
                ruleParser = new SingleTableRuleParser();
                break;
            case MULTI_TABLE_ACCURACY:
                ruleParser = new MultiTableAccuracyRuleParser();
                break;
            case MULTI_TABLE_COMPARISON:
                ruleParser = new MultiTableComparisonRuleParser();
                break;
            default:
                throw  new Exception("rule type is not support");
        }

        return ruleParser.parse(ruleDefinition,inputParameterValueResult,dataQualityTaskExecutionContext);
    }

    public static void main(String[] args) throws Exception{
//        testMultiTableAccuracy();
//        testMultiTableComparison();
        testSingleTable();
    }

    private static void testSingleTable() throws Exception{
        RuleDefinition ruleDefinition = new RuleDefinition();
        ruleDefinition.setRuleName("空值检测");
        ruleDefinition.setRuleType(RuleType.SINGLE_TABLE);

        List<RuleInputEntry> defaultInputEntryList = new ArrayList<>();

        RuleInputEntry srcConnectorType = new RuleInputEntry();
        srcConnectorType.setTitle("源数据类型");
        srcConnectorType.setField("src_connector_type");
        srcConnectorType.setType(FormType.SELECT);
        srcConnectorType.setCanEdit(true);
        srcConnectorType.setShow(true);
        srcConnectorType.setValue("JDBC");
        srcConnectorType.setPlaceholder("${src_connector_type}");
        srcConnectorType.setOptionSourceType(OptionSourceType.DEFAULT);
        srcConnectorType.setOptions("[{label:\"HIVE\",value:\"HIVE\"},{label:\"JDBC\",value:\"JDBC\"}]");

        RuleInputEntry srcDatasourceId = new RuleInputEntry();
        srcDatasourceId.setTitle("源数据ID");
        srcDatasourceId.setField("src_datasource_id");
        srcDatasourceId.setType(FormType.CASCADER);
        srcDatasourceId.setCanEdit(true);
        srcDatasourceId.setShow(true);
        srcDatasourceId.setPlaceholder("${comparison_value}");
        srcDatasourceId.setOptionSourceType(OptionSourceType.DATASOURCE);

        RuleInputEntry srcTable = new RuleInputEntry();
        srcTable.setTitle("源数据表名");
        srcTable.setField("src_table");
        srcTable.setType(FormType.INPUT);
        srcTable.setCanEdit(true);
        srcTable.setShow(true);
        srcTable.setPlaceholder("${src_table}");
        srcTable.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry srcFilter = new RuleInputEntry();
        srcFilter.setTitle("源数据过滤条件");
        srcFilter.setField("src_filter");
        srcFilter.setType(FormType.INPUT);
        srcFilter.setCanEdit(true);
        srcFilter.setShow(true);
        srcFilter.setPlaceholder("${src_filter}");
        srcFilter.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry srcField = new RuleInputEntry();
        srcField.setTitle("源数据过滤条件");
        srcField.setField(SRC_FIELD);
        srcField.setType(FormType.INPUT);
        srcField.setCanEdit(true);
        srcField.setShow(true);
        srcField.setValue(" ");
        srcField.setPlaceholder("${src_field}");
        srcField.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry statisticsName = new RuleInputEntry();
        statisticsName.setTitle("统计指标名");
        statisticsName.setField("statistics_name");
        statisticsName.setType(FormType.INPUT);
        statisticsName.setCanEdit(false);
        statisticsName.setShow(false);
        statisticsName.setValue("miss_items.miss");
        statisticsName.setPlaceholder("${statistics_name}");
        statisticsName.setOptionSourceType(OptionSourceType.DEFAULT);

        defaultInputEntryList.add(srcConnectorType);
        defaultInputEntryList.add(srcDatasourceId);
        defaultInputEntryList.add(srcTable);
        defaultInputEntryList.add(srcFilter);
        defaultInputEntryList.add(srcField);
        defaultInputEntryList.add(statisticsName);

        ruleDefinition.setRuleInputEntryList(defaultInputEntryList);

        List<ExecuteSqlDefinition> statisticsExecuteSqlList = new ArrayList<>();
        ExecuteSqlDefinition executeSqlDefinition2 = new ExecuteSqlDefinition();
        executeSqlDefinition2.setIndex(0);
        executeSqlDefinition2.setSql("SELECT count(*) AS miss FROM ${src_table} WHERE (${src_field} is null) AND (${src_filter}) ");
        executeSqlDefinition2.setTableAlias("miss_items");
        statisticsExecuteSqlList.add(executeSqlDefinition2);
        ruleDefinition.setStatisticsExecuteSqlList(statisticsExecuteSqlList);

        ruleDefinition.setComparisonValueType(ComparisonValueType.CALCULATE_VALUE);

        CalculateComparisonValueParameter calculateComparisonValueParameter = new CalculateComparisonValueParameter();

        List<ExecuteSqlDefinition> comparisonExecuteSqlList = new ArrayList<>();
        ExecuteSqlDefinition executeSqlDefinition3 = new ExecuteSqlDefinition();
        executeSqlDefinition3.setIndex(0);
        executeSqlDefinition3.setSql("SELECT COUNT(*) AS total FROM ${src_table} WHERE (${src_filter})");
        executeSqlDefinition3.setTableAlias("total_count");
        comparisonExecuteSqlList.add(executeSqlDefinition3);
        calculateComparisonValueParameter.setComparisonExecuteSqlList(comparisonExecuteSqlList);

        List<RuleInputEntry> comparisonInputEntryList = new ArrayList<>();
        RuleInputEntry comparisonTitle = new RuleInputEntry();
        comparisonTitle.setTitle("比对规则名称");
        comparisonTitle.setField("comparison_title");
        comparisonTitle.setType(FormType.INPUT);
        comparisonTitle.setCanEdit(false);
        comparisonTitle.setShow(true);
        comparisonTitle.setPlaceholder("${comparison_title}");

        RuleInputEntry comparisonValue = new RuleInputEntry();
        comparisonValue.setTitle("比对值");
        comparisonValue.setField("comparison_value");
        comparisonValue.setType(FormType.INPUT);
        comparisonValue.setCanEdit(false);
        comparisonValue.setShow(true);
        comparisonValue.setPlaceholder("${comparison_value}");

        RuleInputEntry comparisonName = new RuleInputEntry();
        comparisonName.setTitle("比对值名");
        comparisonName.setField("comparison_name");
        comparisonName.setType(FormType.INPUT);
        comparisonName.setCanEdit(false);
        comparisonName.setShow(true);
        comparisonName.setValue("total_count.total");
        comparisonName.setPlaceholder("${comparison_name}");

        comparisonInputEntryList.add(comparisonTitle);
        comparisonInputEntryList.add(comparisonValue);
        comparisonInputEntryList.add(comparisonName);

        calculateComparisonValueParameter.setInputEntryList(comparisonInputEntryList);

        ruleDefinition.setComparisonParameter(JSONUtils.toJsonString(calculateComparisonValueParameter));

        Map<String,String> inputParameterValue = new HashMap<>();
        inputParameterValue.put("src_connector_type","JDBC");
        inputParameterValue.put("src_datasource_id","1");
        inputParameterValue.put("src_table","test1");
        inputParameterValue.put("src_filter","date=2012-10-05");
        inputParameterValue.put("src_field","id");

        inputParameterValue.put("rule_type","1");
        inputParameterValue.put("task_id","1");
        inputParameterValue.put("task_instance_id","1");
        inputParameterValue.put("check_type","1");
        inputParameterValue.put("threshold","1");
        inputParameterValue.put("create_time","222222");
        inputParameterValue.put("update_time","333333");

        DataQualityTaskExecutionContext dataQualityTaskExecutionContext = new DataQualityTaskExecutionContext();
        dataQualityTaskExecutionContext.setDataSourceId(1);
        dataQualityTaskExecutionContext.setSourceConnectorType("JDBC");
        dataQualityTaskExecutionContext.setSourceType(0);
        dataQualityTaskExecutionContext.setSourceConnectionParams("{\"address\":\"jdbc:mysql://localhost:3306\",\"database\":\"test\",\"jdbcUrl\":\"jdbc:mysql://localhost:3306/test\",\"user\":\"test\",\"password\":\"test\",\"other\":\"autoReconnect=true\"}");

//        dataQualityTaskExecutionContext.setDataTargetId(1);
//        dataQualityTaskExecutionContext.setTargetConnectorType("HIVE");
//        dataQualityTaskExecutionContext.setTargetType(2);
//        dataQualityTaskExecutionContext.setTargetConnectionParams("{\"address\":\"jdbc:mysql://localhost:3306\",\"database\":\"default\",\"jdbcUrl\":\"jdbc:mysql://localhost:3306/test\",\"user\":\"test\",\"password\":\"test\",\"other\":\"autoReconnect=true\"}");

        dataQualityTaskExecutionContext.setWriterType(0);
        dataQualityTaskExecutionContext.setWriterConnectorType("JDBC");
        dataQualityTaskExecutionContext.setWriterTable("dqs_result");
        dataQualityTaskExecutionContext.setSourceConnectionParams("{\"address\":\"jdbc:mysql://localhost:3306\",\"database\":\"test\",\"jdbcUrl\":\"jdbc:mysql://localhost:3306/test\",\"user\":\"test\",\"password\":\"test\",\"other\":\"autoReconnect=true\"}");

        RuleManager ruleManager = new RuleManager(JSONUtils.toJsonString(ruleDefinition),inputParameterValue,dataQualityTaskExecutionContext);
        System.out.println(JSONUtils.toJsonString(ruleManager.generateDataQualityParameter()));

    }

    private static void testMultiTableComparison() throws Exception{
        RuleDefinition ruleDefinition = new RuleDefinition();
        ruleDefinition.setRuleName("跨表值比对");
        ruleDefinition.setRuleType(RuleType.MULTI_TABLE_COMPARISON);

        List<RuleInputEntry> defaultInputEntryList = new ArrayList<>();

        RuleInputEntry srcConnectorType = new RuleInputEntry();
        srcConnectorType.setTitle("源数据类型");
        srcConnectorType.setField("src_connector_type");
        srcConnectorType.setType(FormType.SELECT);
        srcConnectorType.setCanEdit(true);
        srcConnectorType.setShow(true);
        srcConnectorType.setValue("JDBC");
        srcConnectorType.setPlaceholder("${src_connector_type}");
        srcConnectorType.setOptionSourceType(OptionSourceType.DEFAULT);
        srcConnectorType.setOptions("[{label:\"HIVE\",value:\"HIVE\"},{label:\"JDBC\",value:\"JDBC\"}]");

        RuleInputEntry srcDatasourceId = new RuleInputEntry();
        srcDatasourceId.setTitle("源数据ID");
        srcDatasourceId.setField("src_datasource_id");
        srcDatasourceId.setType(FormType.CASCADER);
        srcDatasourceId.setCanEdit(true);
        srcDatasourceId.setShow(true);
        srcDatasourceId.setPlaceholder("${comparison_value}");
        srcDatasourceId.setOptionSourceType(OptionSourceType.DATASOURCE);

        RuleInputEntry srcTable = new RuleInputEntry();
        srcTable.setTitle("源数据表名");
        srcTable.setField("src_table");
        srcTable.setType(FormType.INPUT);
        srcTable.setCanEdit(true);
        srcTable.setShow(true);
        srcTable.setPlaceholder("${src_table}");
        srcTable.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry statisticsName = new RuleInputEntry();
        statisticsName.setTitle("统计指标名");
        statisticsName.setField("statistics_name");
        statisticsName.setType(FormType.INPUT);
        statisticsName.setCanEdit(true);
        statisticsName.setShow(true);
        statisticsName.setPlaceholder("${statistics_name}");
        statisticsName.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry statisticsExecuteSql = new RuleInputEntry();
        statisticsExecuteSql.setTitle("统计指标计算SQL");
        statisticsExecuteSql.setField("statistics_execute_sql");
        statisticsExecuteSql.setType(FormType.INPUT);
        statisticsExecuteSql.setCanEdit(true);
        statisticsExecuteSql.setShow(true);
        statisticsExecuteSql.setPlaceholder("${statistics_execute_sql}");
        statisticsExecuteSql.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry targetConnectorType = new RuleInputEntry();
        targetConnectorType.setTitle("目标数据类型");
        targetConnectorType.setField("target_connector_type");
        targetConnectorType.setType(FormType.SELECT);
        targetConnectorType.setCanEdit(true);
        targetConnectorType.setShow(true);
        targetConnectorType.setValue("JDBC");
        targetConnectorType.setPlaceholder("${target_connector_type}");
        targetConnectorType.setOptionSourceType(OptionSourceType.DEFAULT);
        targetConnectorType.setOptions("[{label:\"HIVE\",value:\"HIVE\"},{label:\"JDBC\",value:\"JDBC\"}]");

        RuleInputEntry targetDatasourceId = new RuleInputEntry();
        targetDatasourceId.setTitle("目标数据源ID");
        targetDatasourceId.setField("target_datasource_id");
        targetDatasourceId.setType(FormType.CASCADER);
        targetDatasourceId.setCanEdit(true);
        targetDatasourceId.setShow(true);
        targetDatasourceId.setPlaceholder("${target_datasource_id}");
        targetDatasourceId.setOptionSourceType(OptionSourceType.DATASOURCE);

        RuleInputEntry targetTable = new RuleInputEntry();
        targetTable.setTitle("目标数据表名");
        targetTable.setField("target_table");
        targetTable.setType(FormType.INPUT);
        targetTable.setCanEdit(true);
        targetTable.setShow(true);
        targetTable.setPlaceholder("${target_table}");
        targetTable.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry comparisonName = new RuleInputEntry();
        comparisonName.setTitle("比对值名");
        comparisonName.setField("comparison_name");
        comparisonName.setType(FormType.INPUT);
        comparisonName.setCanEdit(true);
        comparisonName.setShow(true);
        comparisonName.setPlaceholder("${comparison_name}");
        comparisonName.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry comparisonExecuteSql = new RuleInputEntry();
        comparisonExecuteSql.setTitle("比对值计算SQL");
        comparisonExecuteSql.setField("comparison_execute_sql");
        comparisonExecuteSql.setType(FormType.INPUT);
        comparisonExecuteSql.setCanEdit(true);
        comparisonExecuteSql.setShow(true);
        comparisonExecuteSql.setPlaceholder("${comparison_execute_sql}");
        comparisonExecuteSql.setOptionSourceType(OptionSourceType.DEFAULT);

        defaultInputEntryList.add(srcConnectorType);
        defaultInputEntryList.add(srcDatasourceId);
        defaultInputEntryList.add(srcTable);
        defaultInputEntryList.add(statisticsName);
        defaultInputEntryList.add(statisticsExecuteSql);

        defaultInputEntryList.add(targetConnectorType);
        defaultInputEntryList.add(targetDatasourceId);
        defaultInputEntryList.add(targetTable);
        defaultInputEntryList.add(comparisonName);
        defaultInputEntryList.add(comparisonExecuteSql);

        ruleDefinition.setRuleInputEntryList(defaultInputEntryList);

        Map<String,String> inputParameterValue = new HashMap<>();
        inputParameterValue.put("src_connector_type","JDBC");
        inputParameterValue.put("src_datasource_id","1");
        inputParameterValue.put("src_table","test1");
        inputParameterValue.put("statistics_name","count1");
        inputParameterValue.put("statistics_execute_sql","select count(1) as count1 from test.test1");

        inputParameterValue.put("target_connector_type","HIVE");
        inputParameterValue.put("target_datasource_id","1");
        inputParameterValue.put("target_table","test1_1");
        inputParameterValue.put("comparison_name","count2");
        inputParameterValue.put("comparison_execute_sql","select count(1) as count2 from default.test1_1");

        inputParameterValue.put("rule_type","1");
        inputParameterValue.put("task_id","1");
        inputParameterValue.put("task_instance_id","1");
        inputParameterValue.put("check_type","1");
        inputParameterValue.put("threshold","1");
        inputParameterValue.put("create_time","222222");
        inputParameterValue.put("update_time","333333");

        DataQualityTaskExecutionContext dataQualityTaskExecutionContext = new DataQualityTaskExecutionContext();
        dataQualityTaskExecutionContext.setDataSourceId(1);
        dataQualityTaskExecutionContext.setSourceConnectorType("JDBC");
        dataQualityTaskExecutionContext.setSourceType(0);
        dataQualityTaskExecutionContext.setSourceConnectionParams("{\"address\":\"jdbc:mysql://localhost:3306\",\"database\":\"test\",\"jdbcUrl\":\"jdbc:mysql://localhost:3306/test\",\"user\":\"test\",\"password\":\"test\",\"other\":\"autoReconnect=true\"}");

        dataQualityTaskExecutionContext.setDataTargetId(1);
        dataQualityTaskExecutionContext.setTargetConnectorType("HIVE");
        dataQualityTaskExecutionContext.setTargetType(2);
        dataQualityTaskExecutionContext.setTargetConnectionParams("{\"address\":\"jdbc:mysql://localhost:3306\",\"database\":\"default\",\"jdbcUrl\":\"jdbc:mysql://localhost:3306/test\",\"user\":\"test\",\"password\":\"test\",\"other\":\"autoReconnect=true\"}");

        dataQualityTaskExecutionContext.setWriterType(0);
        dataQualityTaskExecutionContext.setWriterConnectorType("JDBC");
        dataQualityTaskExecutionContext.setWriterTable("dqs_result");
        dataQualityTaskExecutionContext.setSourceConnectionParams("{\"address\":\"jdbc:mysql://localhost:3306\",\"database\":\"test\",\"jdbcUrl\":\"jdbc:mysql://localhost:3306/test\",\"user\":\"test\",\"password\":\"test\",\"other\":\"autoReconnect=true\"}");

        RuleManager ruleManager = new RuleManager(JSONUtils.toJsonString(ruleDefinition),inputParameterValue,dataQualityTaskExecutionContext);
        System.out.println(JSONUtils.toJsonString(ruleManager.generateDataQualityParameter()));
    }

    private static void testMultiTableAccuracy() throws Exception {
        RuleDefinition ruleDefinition = new RuleDefinition();
        ruleDefinition.setRuleName("跨表准确性");
        ruleDefinition.setRuleType(RuleType.MULTI_TABLE_ACCURACY);

        List<RuleInputEntry> defaultInputEntryList = new ArrayList<>();

        RuleInputEntry srcConnectorType = new RuleInputEntry();
        srcConnectorType.setTitle("源数据类型");
        srcConnectorType.setField("src_connector_type");
        srcConnectorType.setType(FormType.SELECT);
        srcConnectorType.setCanEdit(true);
        srcConnectorType.setShow(true);
        srcConnectorType.setValue("JDBC");
        srcConnectorType.setPlaceholder("${src_connector_type}");
        srcConnectorType.setOptionSourceType(OptionSourceType.DEFAULT);
        srcConnectorType.setOptions("[{label:\"HIVE\",value:\"HIVE\"},{label:\"JDBC\",value:\"JDBC\"}]");

        RuleInputEntry srcDatasourceId = new RuleInputEntry();
        srcDatasourceId.setTitle("源数据ID");
        srcDatasourceId.setField("src_datasource_id");
        srcDatasourceId.setType(FormType.CASCADER);
        srcDatasourceId.setCanEdit(true);
        srcDatasourceId.setShow(true);
        srcDatasourceId.setPlaceholder("${comparison_value}");
        srcDatasourceId.setOptionSourceType(OptionSourceType.DATASOURCE);

        RuleInputEntry srcTable = new RuleInputEntry();
        srcTable.setTitle("源数据表名");
        srcTable.setField("src_table");
        srcTable.setType(FormType.INPUT);
        srcTable.setCanEdit(true);
        srcTable.setShow(true);
        srcTable.setPlaceholder("${src_table}");
        srcTable.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry srcFilter = new RuleInputEntry();
        srcFilter.setTitle("源数据过滤条件");
        srcFilter.setField("src_filter");
        srcFilter.setType(FormType.INPUT);
        srcFilter.setCanEdit(true);
        srcFilter.setShow(true);
        srcFilter.setPlaceholder("${src_filter}");
        srcFilter.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry targetConnectorType = new RuleInputEntry();
        targetConnectorType.setTitle("目标数据类型");
        targetConnectorType.setField("target_connector_type");
        targetConnectorType.setType(FormType.SELECT);
        targetConnectorType.setCanEdit(true);
        targetConnectorType.setShow(true);
        targetConnectorType.setValue("JDBC");
        targetConnectorType.setPlaceholder("${target_connector_type}");
        targetConnectorType.setOptionSourceType(OptionSourceType.DEFAULT);
        targetConnectorType.setOptions("[{label:\"HIVE\",value:\"HIVE\"},{label:\"JDBC\",value:\"JDBC\"}]");

        RuleInputEntry targetDatasourceId = new RuleInputEntry();
        targetDatasourceId.setTitle("目标数据源ID");
        targetDatasourceId.setField("target_datasource_id");
        targetDatasourceId.setType(FormType.CASCADER);
        targetDatasourceId.setCanEdit(true);
        targetDatasourceId.setShow(true);
        targetDatasourceId.setPlaceholder("${target_datasource_id}");
        targetDatasourceId.setOptionSourceType(OptionSourceType.DATASOURCE);

        RuleInputEntry targetTable = new RuleInputEntry();
        targetTable.setTitle("目标数据表名");
        targetTable.setField("target_table");
        targetTable.setType(FormType.INPUT);
        targetTable.setCanEdit(true);
        targetTable.setShow(true);
        targetTable.setPlaceholder("${target_table}");
        targetTable.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry targetFilter = new RuleInputEntry();
        targetFilter.setTitle("目标数据过滤条件");
        targetFilter.setField("target_filter");
        targetFilter.setType(FormType.INPUT);
        targetFilter.setCanEdit(true);
        targetFilter.setShow(true);
        targetFilter.setPlaceholder("${target_filter}");
        targetFilter.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry mappingColumns = new RuleInputEntry();
        mappingColumns.setTitle("映射列名");
        mappingColumns.setField("mapping_columns");
        mappingColumns.setType(FormType.INPUT);
        mappingColumns.setCanEdit(true);
        mappingColumns.setShow(true);
        mappingColumns.setPlaceholder("${mapping_columns}");
        mappingColumns.setOptionSourceType(OptionSourceType.DEFAULT);

        RuleInputEntry statisticsName = new RuleInputEntry();
        statisticsName.setTitle("统计指标名");
        statisticsName.setField("statistics_name");
        statisticsName.setType(FormType.INPUT);
        statisticsName.setCanEdit(false);
        statisticsName.setShow(false);
        statisticsName.setValue("miss_items.miss");
        statisticsName.setPlaceholder("${statistics_name}");
        statisticsName.setOptionSourceType(OptionSourceType.DEFAULT);

        defaultInputEntryList.add(srcConnectorType);
        defaultInputEntryList.add(srcDatasourceId);
        defaultInputEntryList.add(srcFilter);
        defaultInputEntryList.add(srcTable);
        defaultInputEntryList.add(targetConnectorType);
        defaultInputEntryList.add(targetDatasourceId);
        defaultInputEntryList.add(targetFilter);
        defaultInputEntryList.add(targetTable);
        defaultInputEntryList.add(mappingColumns);
        defaultInputEntryList.add(statisticsName);

        ruleDefinition.setRuleInputEntryList(defaultInputEntryList);

        List<ExecuteSqlDefinition> midExecuteSqlList = new ArrayList<>();
        ExecuteSqlDefinition executeSqlDefinition1 = new ExecuteSqlDefinition();
        executeSqlDefinition1.setIndex(0);
        executeSqlDefinition1.setSql("SELECT * FROM (SELECT * FROM ${src_table} WHERE (${src_filter})) ${src_table} LEFT JOIN (SELECT * FROM ${target_table} WHERE (${target_filter})) ${target_table} ON ${on_clause} WHERE ${where_clause}");
        executeSqlDefinition1.setTableAlias("miss_items");
        midExecuteSqlList.add(executeSqlDefinition1);
        ruleDefinition.setMidExecuteSqlList(midExecuteSqlList);

        List<ExecuteSqlDefinition> statisticsExecuteSqlList = new ArrayList<>();
        ExecuteSqlDefinition executeSqlDefinition2 = new ExecuteSqlDefinition();
        executeSqlDefinition2.setIndex(0);
        executeSqlDefinition2.setSql("SELECT COUNT(*) AS miss FROM miss_items");
        executeSqlDefinition2.setTableAlias("miss_items");
        statisticsExecuteSqlList.add(executeSqlDefinition2);
        ruleDefinition.setStatisticsExecuteSqlList(statisticsExecuteSqlList);

        ruleDefinition.setComparisonValueType(ComparisonValueType.CALCULATE_VALUE);

        CalculateComparisonValueParameter calculateComparisonValueParameter = new CalculateComparisonValueParameter();

        List<ExecuteSqlDefinition> comparisonExecuteSqlList = new ArrayList<>();
        ExecuteSqlDefinition executeSqlDefinition3 = new ExecuteSqlDefinition();
        executeSqlDefinition3.setIndex(0);
        executeSqlDefinition3.setSql("SELECT COUNT(*) AS total FROM ${target_table} WHERE (${target_filter})");
        executeSqlDefinition3.setTableAlias("total_count");
        comparisonExecuteSqlList.add(executeSqlDefinition3);
        calculateComparisonValueParameter.setComparisonExecuteSqlList(comparisonExecuteSqlList);

        List<RuleInputEntry> comparisonInputEntryList = new ArrayList<>();
        RuleInputEntry comparisonTitle = new RuleInputEntry();
        comparisonTitle.setTitle("比对规则名称");
        comparisonTitle.setField("comparison_title");
        comparisonTitle.setType(FormType.INPUT);
        comparisonTitle.setCanEdit(false);
        comparisonTitle.setShow(true);
        comparisonTitle.setPlaceholder("${comparison_title}");

        RuleInputEntry comparisonValue = new RuleInputEntry();
        comparisonValue.setTitle("比对值");
        comparisonValue.setField("comparison_value");
        comparisonValue.setType(FormType.INPUT);
        comparisonValue.setCanEdit(false);
        comparisonValue.setShow(true);
        comparisonValue.setPlaceholder("${comparison_value}");

        RuleInputEntry comparisonName = new RuleInputEntry();
        comparisonName.setTitle("比对值名");
        comparisonName.setField("comparison_name");
        comparisonName.setType(FormType.INPUT);
        comparisonName.setCanEdit(false);
        comparisonName.setShow(true);
        comparisonName.setValue("total_count.total");
        comparisonName.setPlaceholder("${comparison_name}");

        comparisonInputEntryList.add(comparisonTitle);
        comparisonInputEntryList.add(comparisonValue);
        comparisonInputEntryList.add(comparisonName);

        calculateComparisonValueParameter.setInputEntryList(comparisonInputEntryList);

        ruleDefinition.setComparisonParameter(JSONUtils.toJsonString(calculateComparisonValueParameter));
        ruleDefinition.setMidExecuteSqlList(midExecuteSqlList);

        System.out.println(JSONUtils.toJsonString(ruleDefinition));

        Map<String,String> inputParameterValue = new HashMap<>();
        inputParameterValue.put("src_connector_type","JDBC");
        inputParameterValue.put("src_datasource_id","1");
        inputParameterValue.put("src_table","test1");
        inputParameterValue.put("src_filter","");

        inputParameterValue.put("target_connector_type","HIVE");
        inputParameterValue.put("target_datasource_id","1");
        inputParameterValue.put("target_table","test1_1");
        inputParameterValue.put("target_filter","");
        inputParameterValue.put("mapping_columns","id,company");

        inputParameterValue.put("rule_type","1");
        inputParameterValue.put("task_id","1");
        inputParameterValue.put("task_instance_id","1");
        inputParameterValue.put("check_type","1");
        inputParameterValue.put("threshold","111");
        inputParameterValue.put("create_time","222222");
        inputParameterValue.put("update_time","333333");

        DataQualityTaskExecutionContext dataQualityTaskExecutionContext = new DataQualityTaskExecutionContext();
        dataQualityTaskExecutionContext.setDataSourceId(1);
        dataQualityTaskExecutionContext.setSourceConnectorType("JDBC");
        dataQualityTaskExecutionContext.setSourceType(0);
        dataQualityTaskExecutionContext.setSourceConnectionParams("{\"address\":\"jdbc:mysql://localhost:3306\",\"database\":\"test\",\"jdbcUrl\":\"jdbc:mysql://localhost:3306/test\",\"user\":\"test\",\"password\":\"test\",\"other\":\"autoReconnect=true\"}");

        dataQualityTaskExecutionContext.setDataTargetId(1);
        dataQualityTaskExecutionContext.setTargetConnectorType("HIVE");
        dataQualityTaskExecutionContext.setTargetType(2);
        dataQualityTaskExecutionContext.setTargetConnectionParams("{\"address\":\"jdbc:mysql://localhost:3306\",\"database\":\"default\",\"jdbcUrl\":\"jdbc:mysql://localhost:3306/test\",\"user\":\"test\",\"password\":\"test\",\"other\":\"autoReconnect=true\"}");

        dataQualityTaskExecutionContext.setWriterType(0);
        dataQualityTaskExecutionContext.setWriterConnectorType("JDBC");
        dataQualityTaskExecutionContext.setWriterTable("dqs_result");
        dataQualityTaskExecutionContext.setSourceConnectionParams("{\"address\":\"jdbc:mysql://localhost:3306\",\"database\":\"test\",\"jdbcUrl\":\"jdbc:mysql://localhost:3306/test\",\"user\":\"test\",\"password\":\"test\",\"other\":\"autoReconnect=true\"}");

        RuleManager ruleManager = new RuleManager(JSONUtils.toJsonString(ruleDefinition),inputParameterValue,dataQualityTaskExecutionContext);
        System.out.println(JSONUtils.toJsonString(ruleManager.generateDataQualityParameter()));
    }
}
