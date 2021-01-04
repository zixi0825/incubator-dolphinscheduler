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
package org.apache.dolphinscheduler.dqs.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * SparkParameter
 */
public class SparkParameter implements IParameter {

    @JsonProperty("log.level")
    private String logLevel;

    @JsonProperty("checkpoint.dir")
    private String checkpointDir;

    @JsonProperty("batch.interval")
    private String batchInterval;

    @JsonProperty("process.interval")
    private String processInterval;

    @JsonProperty("config")
    private Map<String, String> config;

    @JsonProperty("init.clear")
    private Boolean initClear;

    public SparkParameter() {
    }

    public SparkParameter(String logLevel, String checkpointDir, String batchInterval, String processInterval, Map<String, String> config, Boolean initClear) {
        this.logLevel = logLevel;
        this.checkpointDir = checkpointDir;
        this.batchInterval = batchInterval;
        this.processInterval = processInterval;
        this.config = config;
        this.initClear = initClear;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getCheckpointDir() {
        return checkpointDir;
    }

    public void setCheckpointDir(String checkpointDir) {
        this.checkpointDir = checkpointDir;
    }

    public String getBatchInterval() {
        return batchInterval;
    }

    public void setBatchInterval(String batchInterval) {
        this.batchInterval = batchInterval;
    }

    public String getProcessInterval() {
        return processInterval;
    }

    public void setProcessInterval(String processInterval) {
        this.processInterval = processInterval;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public Boolean getInitClear() {
        return initClear;
    }

    public void setInitClear(Boolean initClear) {
        this.initClear = initClear;
    }

    @Override
    public void validate() {

    }
}
