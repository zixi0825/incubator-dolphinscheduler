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

package org.apache.dolphinscheduler.server.master.consumer;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.ResourceType;
import org.apache.dolphinscheduler.common.enums.SqoopJobType;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.enums.UdfType;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.process.ResourceInfo;
import org.apache.dolphinscheduler.common.task.AbstractParameters;
import org.apache.dolphinscheduler.common.task.datax.DataxParameters;
import org.apache.dolphinscheduler.common.task.dqs.DataQualityParameters;
import org.apache.dolphinscheduler.common.task.procedure.ProcedureParameters;
import org.apache.dolphinscheduler.common.task.sql.SqlParameters;
import org.apache.dolphinscheduler.common.task.sqoop.SqoopParameters;
import org.apache.dolphinscheduler.common.task.sqoop.sources.SourceMysqlParameter;
import org.apache.dolphinscheduler.common.task.sqoop.targets.TargetMysqlParameter;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.EnumUtils;
import org.apache.dolphinscheduler.common.utils.FileUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.common.utils.TaskParametersUtils;
import org.apache.dolphinscheduler.dao.datasource.MySQLDataSource;
import org.apache.dolphinscheduler.dao.datasource.SpringConnectionFactory;
import org.apache.dolphinscheduler.dao.entity.DataSource;
import org.apache.dolphinscheduler.dao.entity.Resource;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.Tenant;
import org.apache.dolphinscheduler.dao.entity.UdfFunc;
import org.apache.dolphinscheduler.server.builder.TaskExecutionContextBuilder;
import org.apache.dolphinscheduler.server.entity.*;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.dispatch.ExecutorDispatcher;
import org.apache.dolphinscheduler.server.master.dispatch.context.ExecutionContext;
import org.apache.dolphinscheduler.server.master.dispatch.enums.ExecutorType;
import org.apache.dolphinscheduler.server.master.dispatch.exceptions.ExecuteException;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.service.queue.TaskPriority;
import org.apache.dolphinscheduler.service.queue.TaskPriorityQueue;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * TaskUpdateQueue consumer
 */
@Component
public class TaskPriorityQueueConsumer extends Thread {

    /**
     * logger of TaskUpdateQueueConsumer
     */
    private static final Logger logger = LoggerFactory.getLogger(TaskPriorityQueueConsumer.class);

    /**
     * taskUpdateQueue
     */
    @Autowired
    private TaskPriorityQueue<TaskPriority> taskPriorityQueue;

    /**
     * processService
     */
    @Autowired
    private ProcessService processService;

    /**
     * executor dispatcher
     */
    @Autowired
    private ExecutorDispatcher dispatcher;


    @Autowired
    private SpringConnectionFactory springConnectionFactory;

    /**
     * master config
     */
    @Autowired
    private MasterConfig masterConfig;

    @PostConstruct
    public void init() {
        super.setName("TaskUpdateQueueConsumerThread");
        super.start();
    }

    @Override
    public void run() {
        List<TaskPriority> failedDispatchTasks = new ArrayList<>();
        while (Stopper.isRunning()) {
            try {
                int fetchTaskNum = masterConfig.getMasterDispatchTaskNumber();
                failedDispatchTasks.clear();
                for (int i = 0; i < fetchTaskNum; i++) {
                    if (taskPriorityQueue.size() <= 0) {
                        Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                        continue;
                    }
                    // if not task , blocking here
                    TaskPriority taskPriority = taskPriorityQueue.take();
                    boolean dispatchResult = dispatch(taskPriority);
                    if (!dispatchResult) {
                        failedDispatchTasks.add(taskPriority);
                    }
                }
                if (!failedDispatchTasks.isEmpty()) {
                    for (TaskPriority dispatchFailedTask : failedDispatchTasks) {
                        taskPriorityQueue.put(dispatchFailedTask);
                    }
                    // If there are tasks in a cycle that cannot find the worker group,
                    // sleep for 1 second
                    if (taskPriorityQueue.size() <= failedDispatchTasks.size()) {
                        TimeUnit.MILLISECONDS.sleep(Constants.SLEEP_TIME_MILLIS);
                    }
                }

            } catch (Exception e) {
                logger.error("dispatcher task error", e);
            }
        }
    }

    /**
     * dispatch task
     *
     * @param taskPriority taskPriority
     * @return result
     */
    protected boolean dispatch(TaskPriority taskPriority) {
        boolean result = false;
        try {
            int taskInstanceId = taskPriority.getTaskId();
            TaskExecutionContext context = getTaskExecutionContext(taskInstanceId);
            ExecutionContext executionContext = new ExecutionContext(context.toCommand(), ExecutorType.WORKER, context.getWorkerGroup());

            if (taskInstanceIsFinalState(taskInstanceId)) {
                // when task finish, ignore this task, there is no need to dispatch anymore
                return true;
            } else {
                result = dispatcher.dispatch(executionContext);
            }
        } catch (ExecuteException e) {
            logger.error("dispatch error", e);
        }
        return result;
    }

    /**
     * taskInstance is final state
     * success，failure，kill，stop，pause，threadwaiting is final state
     *
     * @param taskInstanceId taskInstanceId
     * @return taskInstance is final state
     */
    public Boolean taskInstanceIsFinalState(int taskInstanceId) {
        TaskInstance taskInstance = processService.findTaskInstanceById(taskInstanceId);
        return taskInstance.getState().typeIsFinished();
    }

    /**
     * get TaskExecutionContext
     *
     * @param taskInstanceId taskInstanceId
     * @return TaskExecutionContext
     */
    protected TaskExecutionContext getTaskExecutionContext(int taskInstanceId) {
        TaskInstance taskInstance = processService.getTaskInstanceDetailByTaskId(taskInstanceId);

        // task type
        TaskType taskType = TaskType.valueOf(taskInstance.getTaskType());

        // task node
        TaskNode taskNode = JSONUtils.parseObject(taskInstance.getTaskJson(), TaskNode.class);

        Integer userId = taskInstance.getProcessDefine() == null ? 0 : taskInstance.getProcessDefine().getUserId();
        Tenant tenant = processService.getTenantForProcess(taskInstance.getProcessInstance().getTenantId(), userId);

        // verify tenant is null
        if (verifyTenantIsNull(tenant, taskInstance)) {
            processService.changeTaskState(taskInstance, ExecutionStatus.FAILURE,
                taskInstance.getStartTime(),
                taskInstance.getHost(),
                null,
                null,
                taskInstance.getId());
            return null;
        }
        // set queue for process instance, user-specified queue takes precedence over tenant queue
        String userQueue = processService.queryUserQueueByProcessInstanceId(taskInstance.getProcessInstanceId());
        taskInstance.getProcessInstance().setQueue(StringUtils.isEmpty(userQueue) ? tenant.getQueue() : userQueue);
        taskInstance.getProcessInstance().setTenantCode(tenant.getTenantCode());
        taskInstance.setExecutePath(getExecLocalPath(taskInstance));
        taskInstance.setResources(getResourceFullNames(taskNode));

        SQLTaskExecutionContext sqlTaskExecutionContext = new SQLTaskExecutionContext();
        DataxTaskExecutionContext dataxTaskExecutionContext = new DataxTaskExecutionContext();
        ProcedureTaskExecutionContext procedureTaskExecutionContext = new ProcedureTaskExecutionContext();
        SqoopTaskExecutionContext sqoopTaskExecutionContext = new SqoopTaskExecutionContext();
        DataQualityTaskExecutionContext dataQualityTaskExecutionContext = new DataQualityTaskExecutionContext();

        // SQL task
        if (taskType == TaskType.SQL) {
            setSQLTaskRelation(sqlTaskExecutionContext, taskNode);
        }

        // DATAX task
        if (taskType == TaskType.DATAX) {
            setDataxTaskRelation(dataxTaskExecutionContext, taskNode);
        }

        // procedure task
        if (taskType == TaskType.PROCEDURE) {
            setProcedureTaskRelation(procedureTaskExecutionContext, taskNode);
        }

        if (taskType == TaskType.SQOOP) {
            setSqoopTaskRelation(sqoopTaskExecutionContext, taskNode);
        }

        if (taskType == TaskType.DATA_QUALITY) {
            setDataQualityTaskRelation(dataQualityTaskExecutionContext, taskNode);
        }

        return TaskExecutionContextBuilder.get()
            .buildTaskInstanceRelatedInfo(taskInstance)
            .buildProcessInstanceRelatedInfo(taskInstance.getProcessInstance())
            .buildProcessDefinitionRelatedInfo(taskInstance.getProcessDefine())
            .buildSQLTaskRelatedInfo(sqlTaskExecutionContext)
            .buildDataxTaskRelatedInfo(dataxTaskExecutionContext)
            .buildProcedureTaskRelatedInfo(procedureTaskExecutionContext)
            .buildSqoopTaskRelatedInfo(sqoopTaskExecutionContext)
            .buildDataQualityTaskRelatedInfo(dataQualityTaskExecutionContext)
            .create();
    }

    /**
     * set procedure task relation
     *
     * @param procedureTaskExecutionContext procedureTaskExecutionContext
     * @param taskNode                      taskNode
     */
    private void setProcedureTaskRelation(ProcedureTaskExecutionContext procedureTaskExecutionContext, TaskNode taskNode) {
        ProcedureParameters procedureParameters = JSONUtils.parseObject(taskNode.getParams(), ProcedureParameters.class);
        int datasourceId = procedureParameters.getDatasource();
        DataSource datasource = processService.findDataSourceById(datasourceId);
        procedureTaskExecutionContext.setConnectionParams(datasource.getConnectionParams());
    }

    /**
     * set datax task relation
     *
     * @param dataxTaskExecutionContext dataxTaskExecutionContext
     * @param taskNode                  taskNode
     */
    protected void setDataxTaskRelation(DataxTaskExecutionContext dataxTaskExecutionContext, TaskNode taskNode) {
        DataxParameters dataxParameters = JSONUtils.parseObject(taskNode.getParams(), DataxParameters.class);

        DataSource dbSource = processService.findDataSourceById(dataxParameters.getDataSource());
        DataSource dbTarget = processService.findDataSourceById(dataxParameters.getDataTarget());

        if (dbSource != null) {
            dataxTaskExecutionContext.setDataSourceId(dataxParameters.getDataSource());
            dataxTaskExecutionContext.setSourcetype(dbSource.getType().getCode());
            dataxTaskExecutionContext.setSourceConnectionParams(dbSource.getConnectionParams());
        }

        if (dbTarget != null) {
            dataxTaskExecutionContext.setDataTargetId(dataxParameters.getDataTarget());
            dataxTaskExecutionContext.setTargetType(dbTarget.getType().getCode());
            dataxTaskExecutionContext.setTargetConnectionParams(dbTarget.getConnectionParams());
        }
    }

    /**
     * set sqoop task relation
     *
     * @param sqoopTaskExecutionContext sqoopTaskExecutionContext
     * @param taskNode                  taskNode
     */
    private void setSqoopTaskRelation(SqoopTaskExecutionContext sqoopTaskExecutionContext, TaskNode taskNode) {
        SqoopParameters sqoopParameters = JSONUtils.parseObject(taskNode.getParams(), SqoopParameters.class);

        // sqoop job type is template set task relation
        if (sqoopParameters.getJobType().equals(SqoopJobType.TEMPLATE.getDescp())) {
            SourceMysqlParameter sourceMysqlParameter = JSONUtils.parseObject(sqoopParameters.getSourceParams(), SourceMysqlParameter.class);
            TargetMysqlParameter targetMysqlParameter = JSONUtils.parseObject(sqoopParameters.getTargetParams(), TargetMysqlParameter.class);

            DataSource dataSource = processService.findDataSourceById(sourceMysqlParameter.getSrcDatasource());
            DataSource dataTarget = processService.findDataSourceById(targetMysqlParameter.getTargetDatasource());

            if (dataSource != null) {
                sqoopTaskExecutionContext.setDataSourceId(dataSource.getId());
                sqoopTaskExecutionContext.setSourcetype(dataSource.getType().getCode());
                sqoopTaskExecutionContext.setSourceConnectionParams(dataSource.getConnectionParams());
            }

            if (dataTarget != null) {
                sqoopTaskExecutionContext.setDataTargetId(dataTarget.getId());
                sqoopTaskExecutionContext.setTargetType(dataTarget.getType().getCode());
                sqoopTaskExecutionContext.setTargetConnectionParams(dataTarget.getConnectionParams());
            }
        }
    }

    /**
     * set data quality task relation
     *
     * @param dataQualityTaskExecutionContext dataQualityTaskExecutionContext
     * @param taskNode                  taskNode
     */
    private void setDataQualityTaskRelation(DataQualityTaskExecutionContext dataQualityTaskExecutionContext, TaskNode taskNode) {
        DataQualityParameters dataQualityParameters = JSONUtils.parseObject(taskNode.getParams(), DataQualityParameters.class);

        if(dataQualityParameters == null){
            return ;
        }

        Map<String,String> config = dataQualityParameters.getRuleInputParameter();

        if(StringUtils.isNotEmpty(config.get(Constants.SRC_DATASOURCE_ID))){
            DataSource dataSource = processService.findDataSourceById(Integer.valueOf(config.get(Constants.SRC_DATASOURCE_ID)));
            if(dataSource != null){
                dataQualityTaskExecutionContext.setSourceConnectorType(config.get(Constants.SRC_CONNECTOR_TYPE));
                dataQualityTaskExecutionContext.setDataSourceId(dataSource.getId());
                dataQualityTaskExecutionContext.setSourceType(dataSource.getType().getCode());
                dataQualityTaskExecutionContext.setSourceConnectionParams(dataSource.getConnectionParams());
            }
        }

        if(StringUtils.isNotEmpty(config.get(Constants.TARGET_DATASOURCE_ID))){
            DataSource dataSource = processService.findDataSourceById(Integer.valueOf(config.get(Constants.TARGET_DATASOURCE_ID)));
            if(dataSource != null){
                dataQualityTaskExecutionContext.setTargetConnectorType(config.get(Constants.TARGET_CONNECTOR_TYPE));
                dataQualityTaskExecutionContext.setDataTargetId(dataSource.getId());
                dataQualityTaskExecutionContext.setTargetType(dataSource.getType().getCode());
                dataQualityTaskExecutionContext.setTargetConnectionParams(dataSource.getConnectionParams());
            }
        }

        MySQLDataSource mySqlDataSource = new MySQLDataSource();
        mySqlDataSource.setUser(springConnectionFactory.dataSource().getUsername());
        mySqlDataSource.setPassword(springConnectionFactory.dataSource().getPassword());

        String url = springConnectionFactory.dataSource().getUrl();
        String cleanUri = url.substring(5);
        URI uri = URI.create(cleanUri);
        mySqlDataSource.setAddress("jdbc:"+uri.getScheme()+"://"+uri.getHost()+":"+uri.getPort());
        mySqlDataSource.setDatabase(uri.getPath().substring(1));
        String[] result = url.split("\\?");
        if(result.length >=2){
            mySqlDataSource.setOther(result[1]);
        }

        dataQualityTaskExecutionContext.setWriterConnectorType("JDBC");
        dataQualityTaskExecutionContext.setWriterConnectionParams(JSONUtils.toJsonString(mySqlDataSource));
        dataQualityTaskExecutionContext.setWriterTable("t_ds_dqs_result");
        dataQualityTaskExecutionContext.setWriterType(0);
    }

    /**
     * set SQL task relation
     *
     * @param sqlTaskExecutionContext sqlTaskExecutionContext
     * @param taskNode                taskNode
     */
    private void setSQLTaskRelation(SQLTaskExecutionContext sqlTaskExecutionContext, TaskNode taskNode) {
        SqlParameters sqlParameters = JSONUtils.parseObject(taskNode.getParams(), SqlParameters.class);
        int datasourceId = sqlParameters.getDatasource();
        DataSource datasource = processService.findDataSourceById(datasourceId);
        sqlTaskExecutionContext.setConnectionParams(datasource.getConnectionParams());

        // whether udf type
        boolean udfTypeFlag = EnumUtils.isValidEnum(UdfType.class, sqlParameters.getType())
            && StringUtils.isNotEmpty(sqlParameters.getUdfs());

        if (udfTypeFlag) {
            String[] udfFunIds = sqlParameters.getUdfs().split(",");
            int[] udfFunIdsArray = new int[udfFunIds.length];
            for (int i = 0; i < udfFunIds.length; i++) {
                udfFunIdsArray[i] = Integer.parseInt(udfFunIds[i]);
            }

            List<UdfFunc> udfFuncList = processService.queryUdfFunListByIds(udfFunIdsArray);
            Map<UdfFunc, String> udfFuncMap = new HashMap<>();
            for (UdfFunc udfFunc : udfFuncList) {
                String tenantCode = processService.queryTenantCodeByResName(udfFunc.getResourceName(), ResourceType.UDF);
                udfFuncMap.put(udfFunc, tenantCode);
            }

            sqlTaskExecutionContext.setUdfFuncTenantCodeMap(udfFuncMap);
        }
    }

    /**
     * get execute local path
     *
     * @return execute local path
     */
    private String getExecLocalPath(TaskInstance taskInstance) {
        return FileUtils.getProcessExecDir(taskInstance.getProcessDefine().getProjectId(),
            taskInstance.getProcessDefine().getId(),
            taskInstance.getProcessInstance().getId(),
            taskInstance.getId());
    }

    /**
     * whehter tenant is null
     *
     * @param tenant       tenant
     * @param taskInstance taskInstance
     * @return result
     */
    protected boolean verifyTenantIsNull(Tenant tenant, TaskInstance taskInstance) {
        if (tenant == null) {
            logger.error("tenant not exists,process instance id : {},task instance id : {}",
                taskInstance.getProcessInstance().getId(),
                taskInstance.getId());
            return true;
        }
        return false;
    }

    /**
     * get resource map key is full name and value is tenantCode
     */
    protected Map<String, String> getResourceFullNames(TaskNode taskNode) {
        Map<String, String> resourcesMap = new HashMap<>();
        AbstractParameters baseParam = TaskParametersUtils.getParameters(taskNode.getType(), taskNode.getParams());

        if (baseParam != null) {
            List<ResourceInfo> projectResourceFiles = baseParam.getResourceFilesList();
            if (CollectionUtils.isNotEmpty(projectResourceFiles)) {

                // filter the resources that the resource id equals 0
                Set<ResourceInfo> oldVersionResources = projectResourceFiles.stream().filter(t -> t.getId() == 0).collect(Collectors.toSet());
                if (CollectionUtils.isNotEmpty(oldVersionResources)) {

                    oldVersionResources.forEach(
                        (t) -> resourcesMap.put(t.getRes(), processService.queryTenantCodeByResName(t.getRes(), ResourceType.FILE))
                    );
                }

                // get the resource id in order to get the resource names in batch
                Stream<Integer> resourceIdStream = projectResourceFiles.stream().map(resourceInfo -> resourceInfo.getId());
                Set<Integer> resourceIdsSet = resourceIdStream.collect(Collectors.toSet());

                if (CollectionUtils.isNotEmpty(resourceIdsSet)) {
                    Integer[] resourceIds = resourceIdsSet.toArray(new Integer[resourceIdsSet.size()]);

                    List<Resource> resources = processService.listResourceByIds(resourceIds);
                    resources.forEach(
                        (t) -> resourcesMap.put(t.getFullName(), processService.queryTenantCodeByResName(t.getFullName(), ResourceType.FILE))
                    );
                }
            }
        }

        return resourcesMap;
    }

}
