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
<template>
  <div class="spark-model">
    <m-list-box>
      <div slot="text">{{$t('Rule Name')}}</div>
      <div slot="content">
        <el-select
                style="width: 130px;"
                size="small"
                v-model="ruleId"
                :disabled="isDetails" 
                @change="_handleRuleChange">
          <el-option
                  v-for="rule in ruleNameList"
                  :key="rule.value"
                  :value="rule.value"
                  :label="rule.label">
          </el-option>
        </el-select>
      </div>
    </m-list-box>
    
    <div class="form-box">
        <form-create v-model="fApi" :rule="rule" :option="option" ></form-create>
    </div>
    <m-list-box>
      <div slot="text">{{$t('Deploy Mode')}}</div>
      <div slot="content">
        <el-radio-group v-model="deployMode" size="small">
          <el-radio :label="'cluster'" :disabled="isDetails"></el-radio>
          <el-radio :label="'client'" :disabled="isDetails"></el-radio>
          <el-radio :label="'local'" :disabled="isDetails"></el-radio>
        </el-radio-group>
      </div>
    </m-list-box>
    <m-list-4-box>
      <div slot="text">{{$t('Driver cores')}}</div>
      <div slot="content">
        <el-input
          :disabled="isDetails"
          type="input"
          size="small"
          v-model="driverCores"
          :placeholder="$t('Please enter Driver cores')">
        </el-input>
      </div>
      <div slot="text-2">{{$t('Driver memory')}}</div>
      <div slot="content-2">
        <el-input
          :disabled="isDetails"
          type="input"
          size="small"
          v-model="driverMemory"
          :placeholder="$t('Please enter Driver memory')">
        </el-input>
      </div>
    </m-list-4-box>
    <m-list-4-box>
      <div slot="text">{{$t('Executor Number')}}</div>
      <div slot="content">
        <el-input
          :disabled="isDetails"
          type="input"
          size="small"
          v-model="numExecutors"
          :placeholder="$t('Please enter Executor number')">
        </el-input>
      </div>
      <div slot="text-2">{{$t('Executor memory')}}</div>
      <div slot="content-2">
        <el-input
          :disabled="isDetails"
          type="input"
          size="small"
          v-model="executorMemory"
          :placeholder="$t('Please enter Executor memory')">
        </el-input>
      </div>
    </m-list-4-box>
    <m-list-4-box>
      <div slot="text">{{$t('Executor cores')}}</div>
      <div slot="content">
        <el-input
          :disabled="isDetails"
          type="input"
          size="small"
          v-model="executorCores"
          :placeholder="$t('Please enter Executor cores')">
        </el-input>
      </div>
    </m-list-4-box>
   
    <m-list-box>
      <div slot="text">{{$t('Other parameters')}}</div>
      <div slot="content">
        <el-input
            :disabled="isDetails"
            :autosize="{minRows:2}"
            type="textarea"
            size="small"
            v-model="others"
            :placeholder="$t('Please enter other parameters')">
        </el-input>
      </div>
    </m-list-box>
    
    <m-list-box>
      <div slot="text">{{$t('Custom Parameters')}}</div>
      <div slot="content">
        <m-local-params
                ref="refLocalParams"
                @on-local-params="_onLocalParams"
                :udp-list="localParams"
                :hide="false">
        </m-local-params>
      </div>
    </m-list-box>
  </div>
</template>
<script>
  import _ from 'lodash'
  import i18n from '@/module/i18n'
  import mLocalParams from './_source/localParams'
  import mListBox from './_source/listBox'
  import mList4Box from './_source/list4Box'
  import formCreate from "@form-create/element-ui"
  import disabledState from '@/module/mixin/disabledState'

  export default {
    name: 'data_quality',
    data () {
      return {
        // Deployment method
        deployMode: 'cluster',
        // Custom function
        localParams: [],
        // Driver Number of cores
        driverCores: 1,
        // Driver Number of memory
        driverMemory: '512M',
        // Executor Number
        numExecutors: 2,
        // Executor Number of memory
        executorMemory: '2G',
        // Executor Number of cores
        executorCores: 2,
        // Other parameters
        others: '',
        // Program type
        ruleId: 0,
        // ruleNameList
        ruleNameList: [],
      
        ruleMap:{},

        sparkParam:{},

        ruleJson:{},

        inputEntryValueMap:{},
       
        normalizer (node) {
          return {
            label: node.name
          }
        },
        rule:[],
        fApi: {},
        option: {
          resetBtn: false,
          submitBtn: false,
          row:{
            gutter:0
          }
        }

      }
      
    },
    props: {
      backfillItem: Object
    },
    mixins: [disabledState],
    methods: {

      _handleRuleChange(o){
        this._getRuluInputEntryList(o)
      },
      /**
       * Get the rule input entry list
       */
      _getRuluInputEntryList(ruleId) {
        return new Promise((resolve, reject) => {
          this.store.dispatch('dag/getRuleInputEntryList',ruleId).then(res => {
              //这里需要空值判断
              this.rule = JSON.parse(res.data)
          })
        })
      },

      /**
       * Get rule list
       */
      _getRuluList () {
        return new Promise((resolve, reject) => {
          this.store.dispatch('dag/getRuleList', 1).then(res => {
              var ruleList = res.data;
              this.ruleNameList = new Array();
              this.ruleMap = new Map()
              res.data.forEach((item,i) => {
                var obj = new Object(); 
                obj.label=item.name 
                obj.value=item.id
                this.ruleNameList.push(obj); 
                this.ruleMap.set(item.id,item.ruleJson)
              })

              if(this.ruleId === 0){
                  this.ruleId = this.ruleNameList[0].value
                  this._getRuluInputEntryList(this.ruleId)
              }else{
                  this._getRuluInputEntryList(this.ruleId)
                  window.setTimeout(() => {
                  var fields = this.fApi.fields();
                  fields.forEach(item =>{
                      // console.log(item)
                      // console.log(this.inputEntryValueMap[item])
                      this.fApi.setValue(item,this.inputEntryValueMap[item])
                    })
                  }, 1000);
              }
          })
        })
      },
      
      /**
       * return localParams
       */
      _onLocalParams (a) {
        this.localParams = a
      },
     
      /**
       * verification
       */
      _verification () {

        if (!this.numExecutors) {
          this.$message.warning(`${i18n.$t('Please enter Executor number')}`)
          return false
        }

        if (!Number.isInteger(parseInt(this.numExecutors))) {
          this.$message.warning(`${i18n.$t('The Executor Number should be a positive integer')}`)
          return false
        }

        if (!this.executorMemory) {
          this.$message.warning(`${i18n.$t('Please enter Executor memory')}`)
          return false
        }

        if (!this.executorMemory) {
          this.$message.warning(`${i18n.$t('Please enter Executor memory')}`)
          return false
        }

        if (!_.isNumber(parseInt(this.executorMemory))) {
          this.$message.warning(`${i18n.$t('Memory should be a positive integer')}`)
          return false
        }

        if (!this.executorCores) {
          this.$message.warning(`${i18n.$t('Please enter Executor cores')}`)
          return false
        }

        if (!Number.isInteger(parseInt(this.executorCores))) {
          this.$message.warning(`${i18n.$t('Core number should be positive integer')}`)
          return false
        }
        // localParams Subcomponent verification
        if (!this.$refs.refLocalParams._verifProp()) {
          return false
        }

        this.sparkParam = {
          deployMode: this.deployMode,
          localParams: this.localParams,
          driverCores: this.driverCores,
          driverMemory: this.driverMemory,
          numExecutors: this.numExecutors,
          executorMemory: this.executorMemory,
          executorCores: this.executorCores,
          others: this.others,
        }

        this.inputEntryValueMap = this.fApi.formData();

        if(this.inputEntryValueMap.src_datasource_id && this._isArrayFn(this.inputEntryValueMap.src_datasource_id)){
            this.inputEntryValueMap.src_datasource_id 
                    = this.inputEntryValueMap.src_datasource_id[1]
        }

        if(this.inputEntryValueMap.target_datasource_id && this._isArrayFn(this.inputEntryValueMap.target_datasource_id)){
            this.inputEntryValueMap.target_datasource_id 
                  = this.inputEntryValueMap.target_datasource_id[1]
        }

        this.ruleJson = this.ruleMap.get(this.ruleId);

        var fields = this.fApi.fields();
        try {
          fields.forEach(item =>{
            console.log(item)
            // console.log(this.inputEntryValueMap[item])
            this.fApi.validateField(item,(errMsg)=>{
                if(errMsg){
                  console.log(errMsg)
                  throw new Error(errMsg);
                }
            });
            
            this.fApi.setValue(item,this.inputEntryValueMap[item])
            
          })
        } catch (error) {
          this.$message.warning(error.message)
          return false;
        }
        
        // storage
        this.$emit('on-params', {
          ruleId: this.ruleId,
          sparkParameters: this.sparkParam,
          ruleJson:this.ruleJson,
          ruleInputParameter:this.inputEntryValueMap
        })
        return true
      },

      _isArrayFn (o) {
        return Object.prototype.toString.call(o) === '[object Array]';
      }
    },

    watch: {
      // Watch the cacheParams
      cacheParams (val) {
        this.$emit('on-cache-params', val)
      },
    },

    computed: {
      
      cacheParams () {
        return {
          ruleId: this.ruleId,
          sparkParameters: this.sparkParam,
          ruleJson:this.ruleJson,
          ruleInputParameter:this.inputEntryValueMap
        }
      }
    },

    created () {
      let o = this.backfillItem

      // Non-null objects represent backfill
      if (!_.isEmpty(o)) { 
        this.deployMode = o.params.sparkParameters.deployMode || ''
        this.driverCores = o.params.sparkParameters.driverCores || 1
        this.driverMemory = o.params.sparkParameters.driverMemory || '512M'
        this.numExecutors = o.params.sparkParameters.numExecutors || 2
        this.executorMemory = o.params.sparkParameters.executorMemory || '2G'
        this.executorCores = o.params.sparkParameters.executorCores || 2
        this.others = o.params.sparkParameters.others
        this.ruleId = o.params.ruleId || 0
        // backfill localParams
        let localParams = o.params.sparkParameters.localParams || []
        if (localParams.length) {
          this.localParams = localParams
        }

        this.ruleJson = o.params.ruleJson
        this.inputEntryValueMap = o.params.ruleInputParameter 
      }
    },

    mounted () {
        this._getRuluList()
    },

    components: { 
      mLocalParams,
      mListBox, 
      mList4Box}
       
  }
</script>

<style lang="scss" rel="stylesheet/scss">
  .form-box{
    margin-left: 13px;
    margin-right: 25px;
  }
  .form-box .el-form-item{
    // margin-top: -5px;
    margin-bottom: -1px
  }
</style>
