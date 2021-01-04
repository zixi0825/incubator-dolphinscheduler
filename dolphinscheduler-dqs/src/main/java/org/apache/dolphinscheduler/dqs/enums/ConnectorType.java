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
package org.apache.dolphinscheduler.dqs.enums;

import java.util.ArrayList;
import java.util.List;

/**
 * ConnectorType
 */
public enum ConnectorType {
    /**
     * JDBC
     * HIVE
     */
    JDBC,
    HIVE;

    public static ConnectorType getType(String name){
        for(ConnectorType type: ConnectorType.values()){
            if(type.name().toLowerCase() .equals(name.toLowerCase())){
                return type;
            }
        }

        return null;
    }

    public static List<ConnectorType> validate(List<String> connectorTypes){

        List<ConnectorType> connectorTypeList = new ArrayList<>();

        for(String type:connectorTypes){
            ConnectorType connectorType = getType(type);
            if(connectorType != null){
                connectorTypeList.add(connectorType);
            }
        }

        return connectorTypeList;
    }

}
