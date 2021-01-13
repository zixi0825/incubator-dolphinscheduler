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

package org.apache.dolphinscheduler.common.form.props;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.dolphinscheduler.common.form.CascaderParamsOptions;

import java.util.List;

/**
 * the props field in form-create`s json rule
 */
public class CascaderParamsProps {

    private List<CascaderParamsOptions> options;

    private String placeholder;

    private boolean changeOnSelect;

    @JsonProperty("data")
    public List<CascaderParamsOptions> getOptions() {
        return options;
    }

    public CascaderParamsProps setOptions(List<CascaderParamsOptions> options) {
        this.options = options;
        return this;
    }

    public CascaderParamsProps setOption(CascaderParamsOptions options) {
        this.options.add(options);
        return this;
    }

    @JsonProperty("placeholder")
    public String getPlaceholder() {
        return placeholder;
    }

    public CascaderParamsProps setPlaceholder(String placeholder) {
        this.placeholder = placeholder;
        return this;
    }

    @JsonProperty("changeOnSelect")
    public boolean getChangeOnSelect() {
        return changeOnSelect;
    }

    public CascaderParamsProps setChangeOnSelect(boolean changeOnSelect) {
        this.changeOnSelect = changeOnSelect;
        return this;
    }
}
