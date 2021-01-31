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
package org.apache.dolphinscheduler.api.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import org.apache.dolphinscheduler.common.enums.RuleType;
import org.apache.dolphinscheduler.dao.entity.DqsResult;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.Map;

/**
 * DqsResultService
 */
public interface DqsResultService {

    Map<String, Object> getByTaskInstanceId(int taskInstanceId);

    Map<String, Object> queryResultListPaging(User loginUser,
                                              String searchVal,
                                              Integer state,
                                              Integer ruleType,
                                              String startTime,
                                              String endTime,
                                              Integer pageNo, Integer pageSize);
}
