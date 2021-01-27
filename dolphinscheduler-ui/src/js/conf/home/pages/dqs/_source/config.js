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

import i18n from '@/module/i18n'

/**
 * Post status
 * @id Front end definition id
 * @desc tooltip
 * @code Backend definition identifier
 */
const ruleType = [
  {
    id: 0,
    desc: `${i18n.$t('Unpublished')}`,
    code: 'NOT_RELEASE'
  },
  {
    id: 1,
    desc: `${i18n.$t('online')}`,
    code: 'ONLINE'
  },
  {
    id: 2,
    desc: `${i18n.$t('offline')}`,
    code: 'OFFLINE'
  }
]

/**
 * Operation type
 * @desc tooltip
 * @code identifier
 */
const runningType = [
  {
    desc: `${i18n.$t('Start Process')}`,
    code: 'START_PROCESS'
  },
  {
    desc: `${i18n.$t('Execute from the current node')}`,
    code: 'START_CURRENT_TASK_PROCESS'
  },
  {
    desc: `${i18n.$t('Recover tolerance fault process')}`,
    code: 'RECOVER_TOLERANCE_FAULT_PROCESS'
  },
  {
    desc: `${i18n.$t('Resume the suspension process')}`,
    code: 'RECOVER_SUSPENDED_PROCESS'
  },
  {
    desc: `${i18n.$t('Execute from the failed nodes')}`,
    code: 'START_FAILURE_TASK_PROCESS'
  },
  {
    desc: `${i18n.$t('Complement Data')}`,
    code: 'COMPLEMENT_DATA'
  },
  {
    desc: `${i18n.$t('Scheduling execution')}`,
    code: 'SCHEDULER'
  },
  {
    desc: `${i18n.$t('Rerun')}`,
    code: 'REPEAT_RUNNING'
  },
  {
    desc: `${i18n.$t('Pause')}`,
    code: 'PAUSE'
  },
  {
    desc: `${i18n.$t('Stop')}`,
    code: 'STOP'
  },
  {
    desc: `${i18n.$t('Recovery waiting thread')}`,
    code: 'RECOVER_WAITTING_THREAD'
  }
]

/**
 * Task status
 * @key key
 * @id id
 * @desc tooltip
 * @color color
 * @icoUnicode iconfont
 * @isSpin is loading (Need to execute the code block to write if judgment)
 */
const state = [
  {
    id: 0,
    desc: `${i18n.$t('Unpublished')}`,
    code: 0
  },
  {
    id: 1,
    desc: `${i18n.$t('online')}`,
    code: 1
  },
  {
    id: 2,
    desc: `${i18n.$t('offline')}`,
    code: 2
  }
]

/**
 * Post status
 * @id Front end definition id
 * @desc tooltip
 * @code Backend definition identifier
 */
const publishStatus = [
  {
    id: 0,
    desc: `${i18n.$t('Unpublished')}`,
    code: 'NOT_RELEASE'
  },
  {
    id: 1,
    desc: `${i18n.$t('online')}`,
    code: 'ONLINE'
  },
  {
    id: 2,
    desc: `${i18n.$t('offline')}`,
    code: 'OFFLINE'
  }
]

/**
 * Node type
 * @key key
 * @desc tooltip
 * @color color (tree and gantt)
 */
const tasksType = {
  SHELL: {
    desc: 'SHELL',
    color: '#646464'
  },
  WATERDROP: {
    desc: 'WATERDROP',
    color: '#646465'
  },
  SUB_PROCESS: {
    desc: 'SUB_PROCESS',
    color: '#0097e0'
  },
  PROCEDURE: {
    desc: 'PROCEDURE',
    color: '#525CCD'
  },
  SQL: {
    desc: 'SQL',
    color: '#7A98A1'
  },
  SPARK: {
    desc: 'SPARK',
    color: '#E46F13'
  },
  FLINK: {
    desc: 'FLINK',
    color: '#E46F13'
  },
  MR: {
    desc: 'MapReduce',
    color: '#A0A5CC'
  },
  PYTHON: {
    desc: 'PYTHON',
    color: '#FED52D'
  },
  DEPENDENT: {
    desc: 'DEPENDENT',
    color: '#2FBFD8'
  },
  HTTP: {
    desc: 'HTTP',
    color: '#E46F13'
  },
  DATAX: {
    desc: 'DataX',
    color: '#1fc747'
  },
  SQOOP: {
    desc: 'SQOOP',
    color: '#E46F13'
  },
  CONDITIONS: {
    desc: 'CONDITIONS',
    color: '#E46F13'
  },
  DATA_QUALITY: {
    desc: 'DATA_QUALITY',
    color: '#E46F13'
  }
}

/**
 * Node type
 * @key key
 * @desc tooltip
 * @color color (tree and gantt)
 */
const checkType = {
  0: {
    id: 0,
    desc: `${i18n.$t('STATISTICS_COMPARE_FIXED_VALUE')}`,
    color: '#646464'
  },
  1: {
    id: 1,
    desc: `${i18n.$t('STATISTICS_COMPARE_COMPARISON')}`,
    color: '#646465'
  },
  2: {
    id: 2,
    desc: `${i18n.$t('STATISTICS_COMPARISON_PERCENTAGE')}`,
    color: '#0097e0'
  }
}

export {
  state
}
