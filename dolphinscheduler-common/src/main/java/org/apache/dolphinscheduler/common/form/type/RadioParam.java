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

package org.apache.dolphinscheduler.common.form.type;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import org.apache.dolphinscheduler.common.enums.FormType;
import org.apache.dolphinscheduler.common.form.ParamsOptions;
import org.apache.dolphinscheduler.common.form.PluginParams;
import org.apache.dolphinscheduler.common.form.Validate;
import org.apache.dolphinscheduler.common.form.props.RadioParamsProps;

/**
 * radio
 */
public class RadioParam extends PluginParams {

    @JsonProperty("options")
    private List<ParamsOptions> paramsOptionsList;

    private RadioParam(Builder builder) {
        super(builder);
        this.paramsOptionsList = builder.paramsOptionsList;
    }

    public static Builder newBuilder(String field, String title) {
        return new RadioParam.Builder(field, title);
    }

    public static class Builder extends PluginParams.Builder {

        private List<ParamsOptions> paramsOptionsList;

        public Builder(String field, String title) {
            super(field, FormType.RADIO, title);
        }

        public Builder addValidate(Validate validate) {
            if (this.validateList == null) {
                this.validateList = new ArrayList<>();
            }
            this.validateList.add(validate);
            return this;
        }

        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        public Builder setTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder setValue(Object value) {
            this.value = value;
            return this;
        }

        public Builder setValidateList(List<Validate> validateList) {
            this.validateList = validateList;
            return this;
        }

        public Builder setParamsOptionsList(List<ParamsOptions> paramsOptionsList) {
            this.paramsOptionsList = paramsOptionsList;
            return this;
        }

        public Builder addParamsOptions(ParamsOptions paramsOptions) {
            if (this.paramsOptionsList == null) {
                this.paramsOptionsList = new ArrayList<>();
            }

            this.paramsOptionsList.add(paramsOptions);
            return this;
        }

        public Builder setProps(RadioParamsProps props) {
            this.props = props;
            return this;
        }

        public RadioParam.Builder setSize(String size){
            if (this.props == null) {
                this.setProps(new RadioParamsProps());
            }

            ((RadioParamsProps)this.props).setSize(size);
            return this;
        }

        @Override
        public RadioParam build() {
            return new RadioParam(this);
        }
    }

    public List<ParamsOptions> getParamsOptionsList() {
        return paramsOptionsList;
    }
}
