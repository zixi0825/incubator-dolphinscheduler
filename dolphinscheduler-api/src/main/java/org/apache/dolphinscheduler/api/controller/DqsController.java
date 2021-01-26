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
package org.apache.dolphinscheduler.api.controller;

import io.swagger.annotations.*;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.AccessTokenService;
import org.apache.dolphinscheduler.api.service.DqsResultService;
import org.apache.dolphinscheduler.api.service.DqsRuleService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.RuleType;
import org.apache.dolphinscheduler.common.utils.ParameterUtils;
import org.apache.dolphinscheduler.dao.entity.DqsRule;
import org.apache.dolphinscheduler.dao.entity.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.HashMap;
import java.util.Map;

import static org.apache.dolphinscheduler.api.enums.Status.*;

/**
 * access token controller
 */
@Api(tags = "DATA_QUALITY_SERVICE", position = 1)
@RestController
@RequestMapping("/dqs")
public class DqsController extends BaseController {

    private static final Logger logger = LoggerFactory.getLogger(DqsController.class);

    @Autowired
    private DqsRuleService dqsRuleService;

    @Autowired
    private DqsResultService dqsResultService;

    @ApiOperation(value = "queryAccessTokenList", notes = "QUERY_ACCESS_TOKEN_LIST_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "searchVal", value = "SEARCH_VAL", dataType = "String"),
            @ApiImplicitParam(name = "pageNo", value = "PAGE_NO", dataType = "Int", example = "1"),
            @ApiImplicitParam(name = "pageSize", value = "PAGE_SIZE", dataType = "Int", example = "20")
    })
    @GetMapping(value = "/getRuleFormCreateJson")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_ACCESSTOKEN_LIST_PAGING_ERROR)
    public Result getRuleFormCreateJsonById(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                              @RequestParam(value = "ruleId") int ruleId){

        Map<String, Object> result = dqsRuleService.getRuleFormCreateJsonById(ruleId);
        return returnDataList(result);
    }

    /**
     * create rule
     *
     * @param loginUser  login user
     * @param name       name
     * @param type       type
     * @param ruleJson   ruleJson
     * @return create result state code
     */
    @ApiIgnore
    @PostMapping(value = "/createRule")
    @ResponseStatus(HttpStatus.CREATED)
    @ApiException(CREATE_ACCESS_TOKEN_ERROR)
    public Result createRule(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                              @RequestParam(value = "name") String name,
                              @RequestParam(value = "type") int type,
                              @RequestParam(value = "ruleJson") String ruleJson) {
        logger.info("login user {} , create rule, name : {} , type : {} , json : {}", loginUser.getUserName(),
                name, type, ruleJson);

        Map<String, Object> result = dqsRuleService.createRule(loginUser, name, type, ruleJson);
        return returnDataList(result);
    }

    /**
     * create rule
     *
     * @param loginUser  login user
     * @param ruleId     ruleId
     * @param name       name
     * @param type       type
     * @param ruleJson   ruleJson
     * @return create result state code
     */
    @ApiIgnore
    @PostMapping(value = "/updateRule")
    @ResponseStatus(HttpStatus.CREATED)
    @ApiException(CREATE_ACCESS_TOKEN_ERROR)
    public Result updateRule(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                             @RequestParam(value = "ruleId") int ruleId,
                             @RequestParam(value = "name") String name,
                             @RequestParam(value = "type") int type,
                             @RequestParam(value = "ruleJson") String ruleJson) {
        logger.info("login user {} , create rule, name : {} , type : {} , json : {}", loginUser.getUserName(),
                name, type, ruleJson);

        Map<String, Object> result = dqsRuleService.updateRule(loginUser, ruleId, name, type, ruleJson);
        return returnDataList(result);
    }

    /**
     * delete rule by id
     *
     * @param loginUser login user
     * @param id        rule id
     * @return delete result code
     */
    @ApiIgnore
    @PostMapping(value = "/deleteRule")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(DELETE_ACCESS_TOKEN_ERROR)
    public Result deleteRuleById(@RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                 @RequestParam(value = "id") int id) {
        logger.info("login user {}, delete rule, id: {},", loginUser.getUserName(), id);
        Map<String, Object> result = dqsRuleService.deleteById(loginUser, id);
        return returnDataList(result);
    }

    /**
     * query rule list paging
     *
     * @param loginUser login user
     * @param searchVal search value
     * @param pageNo page number
     * @param pageSize page size
     * @return rule page
     */
    @ApiOperation(value = "queryRuleListPaging", notes = "QUERY_RULE_LIST_PAGING_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "pageNo", value = "PAGE_NO", required = true, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "searchVal", value = "SEARCH_VAL", required = false, type = "String"),
            @ApiImplicitParam(name = "userId", value = "USER_ID", required = false, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "pageSize", value = "PAGE_SIZE", required = true, dataType = "Int", example = "100")
    })
    @GetMapping(value = "/rulePage")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_PROCESS_DEFINITION_LIST_PAGING_ERROR)
    public Result queryRuleListPaging(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                   @RequestParam("pageNo") Integer pageNo,
                                                   @RequestParam(value = "searchVal", required = false) String searchVal,
                                                   @RequestParam("pageSize") Integer pageSize) {
        logger.info("query rule list paging, login user:{}", loginUser.getUserName());
        Map<String, Object> result = checkPageParams(pageNo, pageSize);
        if (result.get(Constants.STATUS) != Status.SUCCESS) {
            return returnDataListPaging(result);
        }
        searchVal = ParameterUtils.handleEscapes(searchVal);
        result = dqsRuleService.queryDqsRuleListPage(loginUser, searchVal, pageNo, pageSize);
        return returnDataListPaging(result);
    }


    /**
     * query all rule list
     * @return rule list
     */
    @ApiOperation(value = "queryRuleList", notes = "QUERY_RULE_LIST_PAGING_NOTES")
    @GetMapping(value = "/ruleList")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_PROCESS_DEFINITION_LIST_PAGING_ERROR)
    public Result queryRuleList() {
        Map<String, Object> result = dqsRuleService.queryAllRuleList();
        return returnDataList(result);
    }

    /**
     * query rule list paging
     *
     * @param loginUser login user
     * @param searchVal search value
     * @param pageNo page number
     * @param pageSize page size
     * @return rule page
     */
    @ApiOperation(value = "queryRuleListPaging", notes = "QUERY_RULE_LIST_PAGING_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "pageNo", value = "PAGE_NO", required = true, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "searchVal", value = "SEARCH_VAL", required = false, type = "String"),
            @ApiImplicitParam(name = "userId", value = "USER_ID", required = false, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "pageSize", value = "PAGE_SIZE", required = true, dataType = "Int", example = "100")
    })
    @GetMapping(value = "/result/page")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_PROCESS_DEFINITION_LIST_PAGING_ERROR)
    public Result queryDqsResultPaging(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                       @RequestParam(value = "searchVal", required = false) String searchVal,
                                       @RequestParam(value = "ruleType", required = false) Integer ruleType,
                                       @RequestParam(value = "state", required = false) Integer state,
                                       @RequestParam(value = "startDate", required = false) String startTime,
                                       @RequestParam(value = "endDate", required = false) String endTime,
                                       @RequestParam("pageNo") Integer pageNo,
                                       @RequestParam("pageSize") Integer pageSize) {
        logger.info("query dqs result paging, login user:{}", loginUser.getUserName());
        Map<String, Object> result = checkPageParams(pageNo, pageSize);
        if (result.get(Constants.STATUS) != Status.SUCCESS) {
            return returnDataListPaging(result);
        }
        searchVal = ParameterUtils.handleEscapes(searchVal);
        result = dqsResultService.queryDefineListPaging(loginUser, searchVal, state, ruleType, startTime, endTime, pageNo, pageSize);
        return returnDataListPaging(result);
    }



}
