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
package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;

@TableName("t_ds_dqs_result")
public class DqsResult {
    /**
     * primary key
     */
    @TableId(value = "id", type = IdType.AUTO)
    private int id;
    /**
     * task_id
     */
    @TableField(value = "process_defined_id")
    private long processDefinedId;
    /**
     * task_instance_id
     */
    @TableField(value = "task_instance_id")
    private long taskInstanceId;
    /**
     * rule_type
     */
    @TableField(value = "rule_type")
    private int ruleType;
    /**
     * statistics_value
     */
    @TableField(value = "statistics_value")
    private double statisticsValue;
    /**
     * comparison_value
     */
    @TableField(value = "comparison_value")
    private double comparisonValue;
    /**
     * check_type
     */
    @TableField(value = "check_type")
    private int checkType;
    /**
     * task_instance_id
     */
    @TableField(value = "threshold")
    private double threshold;
    /**
     * operator
     */
    @TableField(value = "operator")
    private int operator;
    /**
     * create_time
     */
    @TableField(value = "create_time")
    private Date createTime;
    /**
     * update_time
     */
    @TableField(value = "update_time")
    private Date updateTime;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getProcessDefinedId() {
        return processDefinedId;
    }

    public void setProcessDefinedId(long processDefinedId) {
        this.processDefinedId = processDefinedId;
    }

    public long getTaskInstanceId() {
        return taskInstanceId;
    }

    public void setTaskInstanceId(long taskInstanceId) {
        this.taskInstanceId = taskInstanceId;
    }

    public int getRuleType() {
        return ruleType;
    }

    public void setRuleType(int ruleType) {
        this.ruleType = ruleType;
    }

    public double getStatisticsValue() {
        return statisticsValue;
    }

    public void setStatisticsValue(double statisticsValue) {
        this.statisticsValue = statisticsValue;
    }

    public double getComparisonValue() {
        return comparisonValue;
    }

    public void setComparisonValue(double comparisonValue) {
        this.comparisonValue = comparisonValue;
    }

    public int getCheckType() {
        return checkType;
    }

    public void setCheckType(int checkType) {
        this.checkType = checkType;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public int getOperator() {
        return operator;
    }

    public void setOperator(int operator) {
        this.operator = operator;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DqsResult that = (DqsResult) o;

        double dis = 1e-2;
        if (id != that.id) {
            return false;
        }
        if (processDefinedId != that.processDefinedId) {
            return false;
        }
        if (taskInstanceId != that.taskInstanceId) {
            return false;
        }
        if (ruleType != that.ruleType) {
            return false;
        }
        if (Math.abs(statisticsValue-that.statisticsValue) > dis) {
            return false;
        }
        if (Math.abs(comparisonValue-that.comparisonValue) > dis) {
            return false;
        }
        if (checkType != that.checkType) {
            return false;
        }
        if (Math.abs(threshold-that.threshold) > dis) {
            return false;
        }
        if (operator != that.operator) {
            return false;
        }
        if (createTime != null && !createTime.equals(that.createTime)) {
            return false;
        }
        if (updateTime != null && !updateTime.equals(that.updateTime)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "DqsResult{" +
                "id=" + id +
                ", processDefinedId=" + processDefinedId +
                ", taskInstanceId='" + taskInstanceId + '\'' +
                ", ruleType='" + ruleType + '\'' +
                ", statisticsValue=" + statisticsValue +
                ", comparisonValue=" + comparisonValue +
                ", checkType='" + checkType + '\'' +
                ", threshold='" + threshold + '\'' +
                ", operator=" + operator +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}
