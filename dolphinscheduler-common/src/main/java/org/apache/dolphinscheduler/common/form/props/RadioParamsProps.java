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
import org.apache.dolphinscheduler.common.enums.PropsType;

/**
 * the props field in form-create`s json rule
 * size	单选框组尺寸，仅对按钮形式的 Radio 或带有边框的 Radio 有效	string	medium / small / mini	—
 * disabled	是否禁用	boolean	—	false
 * textColor	按钮形式的 Radio 激活时的文本颜色	string	—	#ffffff
 * fill	按钮形式的 Radio 激活时的填充色和边框色	string
 */
public class RadioParamsProps {

    private String size;

    private boolean disabled;

    private String textColor;

    private String fill;

    @JsonProperty("size")
    public String getSize() {
        return size;
    }

    public RadioParamsProps setSize(String size) {
        this.size = size;
        return this;
    }

    @JsonProperty("disabled")
    public boolean getDisabled() {
        return disabled;
    }

    public RadioParamsProps setPlaceholder(boolean disabled) {
        this.disabled = disabled;
        return this;
    }

    @JsonProperty("textColor")
    public String getTextColor() {
        return textColor;
    }

    public RadioParamsProps setTextColor(String textColor) {
        this.textColor = textColor;
        return this;
    }

    @JsonProperty("fill")
    public String getFill() {
        return fill;
    }

    public RadioParamsProps setFill(String fill) {
        this.fill = fill;
        return this;
    }
}
