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
package org.apache.dolphinscheduler.dqs.flow.connector;

import org.apache.dolphinscheduler.dqs.configuration.ConnectorParameter;
import org.apache.spark.sql.SparkSession;

/**
 * HiveBatchConnector
 */
public class HiveBatchConnector implements IConnector {

    private SparkSession sparkSession;

    private ConnectorParameter connectorParameter;

    public HiveBatchConnector(SparkSession sparkSession, ConnectorParameter connectorParameter){
        this.sparkSession = sparkSession;
        this.connectorParameter = connectorParameter;
    }

    @Override
    public void execute() {
        //不做任何处理
    }
}
