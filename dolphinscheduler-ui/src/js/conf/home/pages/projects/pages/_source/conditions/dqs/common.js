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
 * State code table
 */
const ruleType = [
  {
    code: '',
    label: `${i18n.$t('none')}`
  },
  {
    label: `${i18n.$t('Single Table')}`,
    code: 0
  },
  {
    label: `${i18n.$t('Single Table Custon Sql')}`,
    code: 1
  },
  {
    label: `${i18n.$t('Multi Table Accuracy')}`,
    code: 2
  },
  {
    label: `${i18n.$t('Multi Table Comparison')}`,
    code: 3
  }
]

const dqsTaskState = [
  {
    code: '',
    label: `${i18n.$t('none')}`
  },
  {
    label: `${i18n.$t('Default')}`,
    code: 0
  },
  {
    label: `${i18n.$t('Success')}`,
    code: 1
  },
  {
    label: `${i18n.$t('Failure')}`,
    code: 2
  }
]

export {
  ruleType,
  dqsTaskState
}
