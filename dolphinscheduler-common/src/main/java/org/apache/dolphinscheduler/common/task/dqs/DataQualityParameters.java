package org.apache.dolphinscheduler.common.task.dqs;

import org.apache.dolphinscheduler.common.process.ResourceInfo;
import org.apache.dolphinscheduler.common.task.AbstractParameters;
import java.util.List;
import java.util.Map;

/**
 * DataQualityParameters
 */
public class DataQualityParameters extends AbstractParameters {

    private int ruleId;
    private String ruleJson;
    private Map<String,String> ruleInputParameter;

    public int getRuleId() {
        return ruleId;
    }

    public void setRuleId(int ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleJson() {
        return ruleJson;
    }

    public void setRuleJson(String ruleJson) {
        this.ruleJson = ruleJson;
    }

    public Map<String, String> getRuleInputParameter() {
        return ruleInputParameter;
    }

    public void setRuleInputParameter(Map<String, String> ruleInputParameter) {
        this.ruleInputParameter = ruleInputParameter;
    }

    @Override
    public boolean checkParameters() {
        return false;
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return null;
    }
}
