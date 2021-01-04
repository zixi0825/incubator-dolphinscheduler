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
package org.apache.dolphinscheduler.dqs.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Map;
import java.util.TimeZone;

import static com.fasterxml.jackson.databind.DeserializationFeature.*;
import static com.fasterxml.jackson.databind.MapperFeature.REQUIRE_SETTERS_FOR_GETTERS;

/**
 * JSONUtil
 */
public class JSONUtil {

    private static final Logger logger = LoggerFactory.getLogger(JSONUtil.class);

    /**
     * can use static singleton, inject: just make sure to reuse!
     */
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
            .configure(READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
            .configure(REQUIRE_SETTERS_FOR_GETTERS, true)
            .setTimeZone(TimeZone.getDefault());

    private JSONUtil() {
        throw new UnsupportedOperationException("Construct JSONUtils");
    }
    
    
    public static String toJson(Object object,SerializationFeature feature){
        try {
            ObjectWriter writer = MAPPER.writer(feature);
            return writer.writeValueAsString(object);
        } catch (Exception e) {
            logger.error("object to json exception!", e);
        }
        return null;
    }

    public static String toJson(Object object){
        try {
            return MAPPER.writeValueAsString(object);
        } catch (Exception e) {
            logger.error("object to json exception!", e);
        }
        return null;
    }
    
    public static <T> T fromJson(String json, Class<T> clazz) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        try {
            return MAPPER.readValue(json, clazz);
        } catch (Exception e) {
            logger.error("parse object exception!", e);
        }
        return null;
    }
    
    public static <T> T fromJson(InputStream in, Class<T> clazz) {
        
        try {
            return MAPPER.readValue(in, clazz);
        } catch (Exception e) {
            logger.error("parse object exception!", e);
        }
        return null;
    }
    
    public static Map<String, Object> toAnyMap(String json) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        try {
            return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            logger.error("json to map exception!", e);
        }

        return null;
    }
    
}
