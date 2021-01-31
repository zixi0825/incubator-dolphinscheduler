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
package org.apache.dolphinscheduler.api.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.BaseService;
import org.apache.dolphinscheduler.api.service.DqsResultService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.RuleType;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.dao.entity.DqsResult;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.DqsResultMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.dolphinscheduler.common.Constants.DATA_LIST;

/**
 * DqsResultServiceImpl
 */
@Service
public class DqsResultServiceImpl extends BaseService  implements DqsResultService {

    @Autowired
    private DqsResultMapper dqsResultMapper;

    @Override
    public Map<String, Object> getByTaskInstanceId(int taskInstanceId) {

        Map<String, Object> result = new HashMap<>(5);

        DqsResult dqsResult =
                dqsResultMapper.selectOne(new QueryWrapper<DqsResult>().eq("task_instance_id",taskInstanceId));

        result.put(Constants.DATA_LIST, dqsResult);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Map<String, Object> queryResultListPaging(User loginUser,
                                                     String searchVal,
                                                     Integer state,
                                                     Integer ruleType,
                                                     String startTime,
                                                     String endTime,Integer pageNo, Integer pageSize) {
        Map<String, Object> result = new HashMap<>();
        int[] statusArray = null;
        // filter by state
        if (state != null) {
            statusArray = new int[]{state};
        }

        Date start = null;
        Date end = null;
        try {
            if (StringUtils.isNotEmpty(startTime)) {
                start = DateUtils.getScheduleDate(startTime);
            }
            if (StringUtils.isNotEmpty(endTime)) {
                end = DateUtils.getScheduleDate(endTime);
            }
        } catch (Exception e) {
            putMsg(result, Status.REQUEST_PARAMS_NOT_VALID_ERROR, "startTime,endTime");
            return result;
        }

        Page<DqsResult> page = new Page<>(pageNo, pageSize);
        PageInfo<DqsResult> pageInfo = new PageInfo<>(pageNo, pageSize);

        if(ruleType == null){
            ruleType = -1;
        }

        IPage<DqsResult> dqsResultPage =
                dqsResultMapper.queryResultListPaging(
                        page,
                        searchVal,
                        loginUser.getId(),
                        statusArray,
                        ruleType,
                        start,
                        end);

        pageInfo.setTotalCount((int) dqsResultPage.getTotal());
        pageInfo.setLists(dqsResultPage.getRecords());
        result.put(DATA_LIST, pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

}
