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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.BaseService;
import org.apache.dolphinscheduler.api.service.DqsRuleService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.DbType;
import org.apache.dolphinscheduler.common.enums.OptionSourceType;
import org.apache.dolphinscheduler.common.enums.PropsType;
import org.apache.dolphinscheduler.common.form.CascaderParamsOptions;
import org.apache.dolphinscheduler.common.form.ParamsOptions;
import org.apache.dolphinscheduler.common.form.PluginParams;
import org.apache.dolphinscheduler.common.form.Validate;
import org.apache.dolphinscheduler.common.form.props.InputParamsProps;
import org.apache.dolphinscheduler.common.form.type.CascaderParam;
import org.apache.dolphinscheduler.common.form.type.InputParam;
import org.apache.dolphinscheduler.common.form.type.RadioParam;
import org.apache.dolphinscheduler.common.form.type.SelectParam;
import org.apache.dolphinscheduler.common.task.dqs.rule.ComparisonParameter;
import org.apache.dolphinscheduler.common.task.dqs.rule.RuleDefinition;
import org.apache.dolphinscheduler.common.task.dqs.rule.RuleInputEntry;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.dao.entity.DataSource;
import org.apache.dolphinscheduler.dao.entity.DqsRule;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.DataSourceMapper;
import org.apache.dolphinscheduler.dao.mapper.DqsRuleMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.*;

/**
 * DqsRuleServiceImpl
 */
@Service
public class DqsRuleServiceImpl extends BaseService  implements DqsRuleService {

    @Autowired
    private DqsRuleMapper dqsRuleMapper;

    @Autowired
    private DataSourceMapper dataSourceMapper;

    @Override
    public  Map<String, Object> getRuleFormCreateJsonById(int id) {

        Map<String, Object> result = new HashMap<>(5);

        DqsRule dqsRule = dqsRuleMapper.selectById(id);
        if(dqsRule == null){
            return null;
        }

        String ruleJson = dqsRule.getRuleJson();
        RuleDefinition ruleDefinition = JSONUtils.parseObject(ruleJson,RuleDefinition.class);

        if(ruleDefinition == null){
            putMsg(result, Status.EDIT_RESOURCE_FILE_ON_LINE_ERROR);
        }else{
            result.put(Constants.DATA_LIST, getRuleFormCreateJson(ruleDefinition));
            putMsg(result, Status.SUCCESS);
        }

        return result;
    }

    @Override
    public  Map<String, Object> createRule(User loginUser,
                                           String name,
                                           int type,
                                           String ruleJson) {
        Map<String, Object> result = new HashMap<>(5);

        DqsRule entity = new DqsRule();
        entity.setName(name);
        entity.setType(type);
        entity.setRuleJson(ruleJson);
        entity.setUserId(loginUser.getId());
        entity.setCreateTime(new Date());
        entity.setUpdateTime(new Date());

        int insert = dqsRuleMapper.insert(entity);

        if (insert > 0) {
            putMsg(result, Status.SUCCESS);
        } else {
            putMsg(result, Status.CREATE_ACCESS_TOKEN_ERROR);
        }

        return result;
    }

    @Override
    public  Map<String, Object> updateRule(User loginUser,
                                           int ruleId,
                                           String name,
                                           int type,
                                           String ruleJson) {
        Map<String, Object> result = new HashMap<>(5);

        //判断是否有权限进行更新，没有的话直接返回权限不足

        DqsRule entity = dqsRuleMapper.selectById(ruleId);
        if(entity == null){
            //直接返回报错信息
            return null;
        }

        entity.setName(name);
        entity.setType(type);
        entity.setRuleJson(ruleJson);
        entity.setUserId(loginUser.getId());
        entity.setUpdateTime(new Date());

        int update = dqsRuleMapper.updateById(entity);

        if (update > 0) {
            putMsg(result, Status.SUCCESS);
        } else {
            putMsg(result, Status.CREATE_ACCESS_TOKEN_ERROR);
        }

        return result;
    }

    @Override
    public  Map<String, Object> deleteById(User loginUser,int id) {
        Map<String, Object> result = new HashMap<>(5);
        int delete = dqsRuleMapper.deleteById(id);

        if (delete > 0) {
            putMsg(result, Status.SUCCESS);
        } else {
            putMsg(result, Status.CREATE_ACCESS_TOKEN_ERROR);
        }

        return result;
    }

    @Override
    public Map<String, Object> queryDqsRuleListPage(User loginUser,String searchVal, Integer pageNo, Integer pageSize) {
        Map<String, Object> result = new HashMap<>();

        Page<DqsRule> page = new Page<>(pageNo, pageSize);
        IPage<DqsRule> rulePage =
                dqsRuleMapper.selectPage(page,new QueryWrapper<DqsRule>().like(StringUtils.isNotEmpty(searchVal),"name",searchVal));

        PageInfo<DqsRule> pageInfo = new PageInfo<>(pageNo, pageSize);
        pageInfo.setTotalCount((int) rulePage.getTotal());
        pageInfo.setLists(rulePage.getRecords());
        result.put(Constants.DATA_LIST, pageInfo);
        putMsg(result, Status.SUCCESS);

        return result;
    }

    @Override
    public Map<String, Object> queryAllRuleList() {
        Map<String, Object> result = new HashMap<>();

        List<DqsRule> ruleList =
                dqsRuleMapper.selectList(new QueryWrapper<DqsRule>());

        result.put(Constants.DATA_LIST, ruleList);
        putMsg(result, Status.SUCCESS);

        return result;
    }

    private String getRuleFormCreateJson(RuleDefinition ruleDefinition){

        List<RuleInputEntry> defaultInputEntryList = new ArrayList<>();
        List<RuleInputEntry> checkInputEntryList = new ArrayList<>();

        for(RuleInputEntry ruleInputEntry:ruleDefinition.getRuleInputEntryList()){
            if(ruleInputEntry != null){
                switch (ruleInputEntry.getInputType()){
                    case DEFAULT:
                        defaultInputEntryList.add(ruleInputEntry);
                        break;
                    case STATISTICS:
                        break;
                    case COMPARISON:
                        break;
                    case CHECK:
                        checkInputEntryList.add(ruleInputEntry);
                        break;
                    default:
                        break;
                }
            }
        }

        List<RuleInputEntry> allInputEntryList = new ArrayList<>(defaultInputEntryList);

        ComparisonParameter comparisonParameter = ruleDefinition.getComparisonParameter();
        if(comparisonParameter != null){
            allInputEntryList.addAll(comparisonParameter.getInputEntryList());
        }


        allInputEntryList.addAll(checkInputEntryList);

        List<PluginParams> params = new ArrayList<>();

        for(RuleInputEntry inputEntry:allInputEntryList){
            if(inputEntry.getShow()){
                switch (inputEntry.getType()){
                    case INPUT:
                        InputParam inputParam = InputParam
                                        .newBuilder(inputEntry.getField(),inputEntry.getTitle())
                                        .addValidate(Validate.newBuilder()
                                                             .setRequired(true)
                                                             .build())
                                        .setProps(new InputParamsProps().setDisabled(!inputEntry.getCanEdit()))
                                        .setValue(inputEntry.getValue())
                                        .setPlaceholder(inputEntry.getPlaceholder())
                                        .setSize("small")
                                        .build();
                        params.add(inputParam);
                        break;
                    case SELECT:
                        List<ParamsOptions> options = null;

                        if(OptionSourceType.DEFAULT == inputEntry.getOptionSourceType()){
                            String optionStr = inputEntry.getOptions();
                            if(StringUtils.isNotEmpty(optionStr)){
                                options = JSONUtils.toList(optionStr,ParamsOptions.class);
                            }
                        }

                        SelectParam selectParam = SelectParam
                                .newBuilder(inputEntry.getField(),inputEntry.getTitle())
                                .setParamsOptionsList(options)
                                .setValue(inputEntry.getValue())
                                .setSize("small")
//                                .setPlaceholder(inputEntry.getPlaceholder())
                                .build();
                        params.add(selectParam);
                        break;
                    case RADIO:
                        List<ParamsOptions> radioOptions = null;

                        if(OptionSourceType.DEFAULT == inputEntry.getOptionSourceType()){
                            String optionStr = inputEntry.getOptions();
                            if(StringUtils.isNotEmpty(optionStr)){
                                radioOptions = JSONUtils.toList(optionStr,ParamsOptions.class);
                            }
                        }

                        RadioParam radioParam = RadioParam
                                .newBuilder(inputEntry.getField(),inputEntry.getTitle())
                                .setParamsOptionsList(radioOptions)
                                .setValue(inputEntry.getValue())
                                .setSize("small")
                                .build();
                        params.add(radioParam);
                        break;
                    case SWITCH:
                        break;
                    case CASCADER:
                        List<CascaderParamsOptions> cascaderOptions = null;

                        if(OptionSourceType.DEFAULT == inputEntry.getOptionSourceType()){
                            String optionStr = inputEntry.getOptions();
                            if(StringUtils.isNotEmpty(optionStr)){
                                cascaderOptions = JSONUtils.toList(optionStr,CascaderParamsOptions.class);
                            }
                        }else if(OptionSourceType.DATASOURCE == inputEntry.getOptionSourceType()){
                            cascaderOptions = new ArrayList<>();
                            for(DbType dbtype:DbType.values()){
                                CascaderParamsOptions cascaderParamsOptions = new CascaderParamsOptions(dbtype.getDescp(),dbtype.getCode(),false);
                                List<CascaderParamsOptions> children = null;
                                List<DataSource> dataSourceList = dataSourceMapper.listAllDataSourceByType(dbtype.getCode());
                                if(CollectionUtils.isNotEmpty(dataSourceList)){
                                    children = new ArrayList<>();
                                    for(DataSource dataSource: dataSourceList){
                                        CascaderParamsOptions childrenOption =
                                                new CascaderParamsOptions(dataSource.getName(),dataSource.getId(),false);
                                        children.add(childrenOption);
                                    }
                                    cascaderParamsOptions.setChildren(children);
                                    cascaderOptions.add(cascaderParamsOptions);
                                }

                            }
                        }

                        CascaderParam cascaderParam = CascaderParam
                                .newBuilder(inputEntry.getField(),inputEntry.getTitle())
                                .setParamsOptionsList(cascaderOptions)
                                .setValue(Integer.valueOf(inputEntry.getValue()))
                                .setSize("small")
                                .build();
                        params.add(cascaderParam);
                        break;

                    case TEXTAREA:
                        InputParam textareaParam = InputParam
                                .newBuilder(inputEntry.getField(),inputEntry.getTitle())
                                .addValidate(Validate.newBuilder()
                                        .setRequired(true)
                                        .build())
                                .setProps(new InputParamsProps().setDisabled(!inputEntry.getCanEdit()))
                                .setValue(inputEntry.getValue())
                                .setSize("small")
                                .setType(PropsType.TEXTAREA)
                                .setRows(1)
                                .setPlaceholder(inputEntry.getPlaceholder())
                                .build();
                        params.add(textareaParam);
                        break;
                    default:
                        break;
                }
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String result = null;

        try {
            result = mapper.writeValueAsString(params);
        }catch (JsonProcessingException e){
            e.printStackTrace();
        }

        return result;
    }
}
