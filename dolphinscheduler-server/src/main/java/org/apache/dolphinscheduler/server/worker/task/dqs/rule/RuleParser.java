package org.apache.dolphinscheduler.server.worker.task.dqs.rule;

import org.apache.dolphinscheduler.common.enums.*;
import org.apache.dolphinscheduler.dao.datasource.BaseDataSource;
import org.apache.dolphinscheduler.dao.datasource.DataSourceFactory;
import org.apache.dolphinscheduler.server.entity.DataQualityTaskExecutionContext;
import org.apache.dolphinscheduler.server.worker.task.dqs.rule.parameter.*;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.ParameterUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.common.utils.placeholder.BusinessTimeUtils;
import java.util.*;

import static org.apache.dolphinscheduler.common.Constants.*;

/**
 * @author zixi0825
 */
public class RuleParser {

    private String ruleJson;
    private Map<String,String> inputParameterValue;
    private DataQualityTaskExecutionContext dataQualityTaskExecutionContext;

    private static final String  CALCULATE_COMPARISON_WRITER_SQL = "SELECT ${rule_type} as rule_type," +
            "${task_id} as task_id," +
            "${task_instance_id} as task_instance_id," +
            "${statistics_name} AS statistics_value, " +
            "${comparison_name} AS comparison_value," +
            "${check_type} as check_type," +
            "${threshold} as threshold, " +
            "${create_time} as create_time," +
            "${update_time} as update_time " +
            "from ${statistics_table} FULL JOIN ${comparison_table}";

    private static final String  FIXED_COMPARISON_WRITER_SQL = "SELECT ${rule_type} as rule_type," +
            "${task_id} as task_id," +
            "${task_instance_id} as task_instance_id," +
            "${statistics_name} AS statistics_value, " +
            "${comparison_name} AS comparison_value," +
            "${check_type} as check_type," +
            "${threshold} as threshold, " +
            "${create_time} as create_time," +
            "${update_time} as update_time " +
            "from ${statistics_table}";

    private static final String MULTI_TABLE_COMPARISON_SQL = "SELECT ${rule_type} as rule_type," +
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

    public RuleParser(String ruleJson,Map<String,String> inputParameterValue,DataQualityTaskExecutionContext dataQualityTaskExecutionContext){
        this.ruleJson = ruleJson;
        this.inputParameterValue = inputParameterValue;
        this.dataQualityTaskExecutionContext = dataQualityTaskExecutionContext;
    }

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
        int index = 1;

        Map<String,String> inputParameterValueResult = getInputParameterMapFromEntryList(ruleDefinition);
        inputParameterValueResult.putAll(inputParameterValue);

        //对参数中的时间格式进行转换
        inputParameterValueResult.putAll(BusinessTimeUtils.getBusinessTime(CommandType.START_PROCESS,new Date()));

        List<ConnectorParameter> connectorParameterList = getConnectorParameterList();
        List<ExecutorParameter> executorParameterList = new ArrayList<>();
        List<WriterParameter> writerParameterList = null;

        if(RuleType.SINGLE_TABLE == ruleDefinition.getRuleType() || RuleType.MULTI_TABLE_ACCURACY == ruleDefinition.getRuleType()){

            if(RuleType.MULTI_TABLE_ACCURACY == ruleDefinition.getRuleType()){
                String mappingColumns = inputParameterValueResult.get(MAPPING_COLUMNS);
                String[] mappingColumnList = mappingColumns.split(",");

                inputParameterValueResult.put(ON_CLAUSE,getOnClause(mappingColumnList,inputParameterValueResult));
                //get where clause
                inputParameterValueResult.put(WHERE_CLAUSE , getWhereClause(mappingColumnList,inputParameterValueResult));
            }

            ExecuteSqlDefinition statisticsSql = ruleDefinition.getStatisticsExecuteSqlList().get(0);
            inputParameterValueResult.put(STATISTICS_TABLE,statisticsSql.getTableAlias());

            List<ExecuteSqlDefinition> midExecuteSqlDefinitionList = ruleDefinition.getMidExecuteSqlList();
            for(ExecuteSqlDefinition executeSqlDefinition:midExecuteSqlDefinitionList){
                index = setExecutorParameter(
                        index,
                        inputParameterValueResult,
                        executorParameterList,
                        executeSqlDefinition);
            }

            List<ExecuteSqlDefinition> statisticsExecuteSqlDefinitionList
                    = ruleDefinition.getStatisticsExecuteSqlList();
            for(ExecuteSqlDefinition executeSqlDefinition:statisticsExecuteSqlDefinitionList){
                index = setExecutorParameter(
                        index,
                        inputParameterValueResult,
                        executorParameterList,
                        executeSqlDefinition);
            }

            if(ComparisonValueType.CALCULATE_VALUE == ruleDefinition.getComparisonValueType()){
                CalculateComparisonValueParameter calculateComparisonValueParameter =
                        JSONUtils.parseObject(ruleDefinition.getComparisonParameter(),CalculateComparisonValueParameter.class);
                if(calculateComparisonValueParameter == null){
                    return null;
                }

                ExecuteSqlDefinition comparisonSql = calculateComparisonValueParameter.getComparisonExecuteSqlList().get(0);
                for(ExecuteSqlDefinition executeSqlDefinition:calculateComparisonValueParameter.getComparisonExecuteSqlList()){
                    index = setExecutorParameter(
                            index,
                            inputParameterValueResult,
                            executorParameterList,
                            executeSqlDefinition);
                }

                inputParameterValueResult.put(COMPARISON_TABLE,comparisonSql.getTableAlias());
                writerParameterList = getWriterParameterList(ParameterUtils.convertParameterPlaceholders(CALCULATE_COMPARISON_WRITER_SQL,inputParameterValueResult));
            } else if(ComparisonValueType.FIXED_VALUE == ruleDefinition.getComparisonValueType()){
                writerParameterList = getWriterParameterList(
                        ParameterUtils.convertParameterPlaceholders(
                                FIXED_COMPARISON_WRITER_SQL,
                                inputParameterValueResult));
            }

        }else if(RuleType.MULTI_TABLE_COMPARISON == ruleDefinition.getRuleType()){
            writerParameterList = getWriterParameterList(ParameterUtils.convertParameterPlaceholders(MULTI_TABLE_COMPARISON_SQL,inputParameterValueResult));
        }

        return new DataQualityConfiguration("123213",new SparkParameter(),connectorParameterList,writerParameterList,executorParameterList);
    }

    private int setExecutorParameter(int index, Map<String, String> inputParameterValueResult, List<ExecutorParameter> executorParameterList, ExecuteSqlDefinition executeSqlDefinition) {
        ExecutorParameter executorParameter = new ExecutorParameter();
        executorParameter.setIndex(index++ + "");
        executorParameter.setExecuteSql(ParameterUtils.convertParameterPlaceholders(executeSqlDefinition.getSql(),inputParameterValueResult));
        executorParameter.setTableAlias(executeSqlDefinition.getTableAlias());
        executorParameterList.add(executorParameter);
        return index;
    }

    private String getCoalesceString(String table, String column){
        return "coalesce("+table+"."+column+", '')";
    }

    private  String getSrcColumnIsNullStr(String table,String[] columns){
        String[] columnList = new String[columns.length];
        for(int i=0; i<columns.length;i++){
            String column = columns[i];
            columnList[i] = table + "." + column + " IS NULL";
        }
        return  String.join(AND ,columnList);
    }

    private Map<String,String> getInputParameterMapFromEntryList(RuleDefinition ruleDefinition){
        List<RuleInputEntry> defaultInputEntryList = ruleDefinition.getRuleInputEntryList();
        if(ComparisonValueType.CALCULATE_VALUE == ruleDefinition.getComparisonValueType()){
            CalculateComparisonValueParameter calculateComparisonValueParameter =
                    JSONUtils.parseObject(ruleDefinition.getComparisonParameter(),CalculateComparisonValueParameter.class);
            if(calculateComparisonValueParameter != null){
                defaultInputEntryList.addAll(calculateComparisonValueParameter.getInputEntryList());
            }

        }else if(ComparisonValueType.FIXED_VALUE == ruleDefinition.getComparisonValueType()){
            FixedComparisonValueParameter fixedComparisonValueParameter =
                    JSONUtils.parseObject(ruleDefinition.getComparisonParameter(),FixedComparisonValueParameter.class);
            if(fixedComparisonValueParameter != null){
                defaultInputEntryList.addAll(fixedComparisonValueParameter.getInputEntryList());
            }
        }

        Map<String,String> defaultInputParameterValue = new HashMap<>();
        for(RuleInputEntry inputEntry:defaultInputEntryList){
            defaultInputParameterValue.put(inputEntry.getField(),inputEntry.getValue());
        }

        return defaultInputParameterValue;
    }

    private List<ConnectorParameter> getConnectorParameterList() throws Exception{

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

    private List<WriterParameter> getWriterParameterList(String sql) throws Exception{

        List<WriterParameter> writerParameterList = new ArrayList<>();

        if(StringUtils.isNotEmpty(dataQualityTaskExecutionContext.getSourceConnectorType())){
            BaseDataSource writerDataSource = DataSourceFactory.getDatasource(DbType.of(dataQualityTaskExecutionContext.getSourceType()),
                    dataQualityTaskExecutionContext.getSourceConnectionParams());
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

    private String getOnClause(String[] mappingColumnList,Map<String,String> inputParameterValueResult){
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

    private String getWhereClause(String[] mappingColumnList,Map<String,String> inputParameterValueResult){
        String srcColumnNotNull = "( NOT (" + getSrcColumnIsNullStr(inputParameterValueResult.get(SRC_TABLE),mappingColumnList) + " ))";
        String targetColumnIsNull = "( " + getSrcColumnIsNullStr(inputParameterValueResult.get(TARGET_TABLE),mappingColumnList) + " )";

        return srcColumnNotNull
                + AND
                + targetColumnIsNull
                + (StringUtils.isEmpty(inputParameterValueResult.get(SRC_FILTER))?"":AND+inputParameterValueResult.get(SRC_FILTER))
                + (StringUtils.isEmpty(inputParameterValueResult.get(TARGET_FILTER))?"":AND+inputParameterValueResult.get(TARGET_FILTER));
    }

    public static void main(String[] args) throws Exception{
        testMultiTableAccuracy();
        testMultiTableComparison();
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

        RuleParser ruleParser = new RuleParser(JSONUtils.toJsonString(ruleDefinition),inputParameterValue,dataQualityTaskExecutionContext);
        System.out.println(JSONUtils.toJsonString(ruleParser.generateDataQualityParameter()));
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
        executeSqlDefinition1.setSql("SELECT * FROM ${src_table} LEFT JOIN ${target_table} ON ${on_clause} WHERE ${where_clause}");
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
        executeSqlDefinition3.setSql("SELECT COUNT(*) AS total FROM ${src_table}");
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

        RuleParser ruleParser = new RuleParser(JSONUtils.toJsonString(ruleDefinition),inputParameterValue,dataQualityTaskExecutionContext);
        System.out.println(JSONUtils.toJsonString(ruleParser.generateDataQualityParameter()));
    }
}
