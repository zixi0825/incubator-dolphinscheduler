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
import org.apache.dolphinscheduler.dqs.utils.JDBCUtil;
import org.apache.dolphinscheduler.dqs.utils.Preconditions;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

import static org.apache.dolphinscheduler.dqs.Constants.*;

/**
 * JDBCBatchConnector
 */
public class JDBCBatchConnector implements IConnector {

    private SparkSession sparkSession;

    private ConnectorParameter connectorParameter;

    public JDBCBatchConnector(SparkSession sparkSession, ConnectorParameter connectorParameter){
        this.sparkSession = sparkSession;
        this.connectorParameter = connectorParameter;
    }

    @Override
    public void execute() {

        Map<String,Object> config = connectorParameter.getConfig();
        String database = String.valueOf(config.getOrDefault(DATABASE,DEFAULT_DATABASE));
        String table = String.valueOf(config.getOrDefault(TABLE,EMPTY));
        String fullTableName = database+"."+table;
        String url = String.valueOf(config.getOrDefault(URL,EMPTY));
        String user = String.valueOf(config.getOrDefault(USER,EMPTY));
        String password = String.valueOf(config.getOrDefault(PASSWORD,EMPTY));
        String driver = String.valueOf(config.getOrDefault(DRIVER,DEFAULT_DRIVER));

        Preconditions.checkArgument(JDBCUtil.isJDBCDriverLoaded(driver), "JDBC driver $driver not present in classpath");

        sparkSession
                .read()
                .format("jdbc")
                .option("driver",driver)
                .option("url",url)
                .option("dbtable", fullTableName)
                .option("user", user)
                .option("password", password)
                .load().createOrReplaceTempView(table);
    }
}
