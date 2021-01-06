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

    @JsonProperty("config")
    private Map<String, String> config;

    public SparkParameter() {
    }

    public SparkParameter(String logLevel,Map<String, String> config) {
        this.logLevel = logLevel;
        this.config = config;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public void validate() {

    }
}
