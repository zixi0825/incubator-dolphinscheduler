package org.apache.dolphinscheduler.server.worker.task.dqs.rule;

import java.util.List;

public class CalculateComparisonValueParameter {
  private List<RuleInputEntry> inputEntryList;
  private List<ExecuteSqlDefinition> comparisonExecuteSqlList;

  public List<RuleInputEntry> getInputEntryList() {
    return inputEntryList;
  }

  public void setInputEntryList(List<RuleInputEntry> inputEntryList) {
    this.inputEntryList = inputEntryList;
  }

    public List<ExecuteSqlDefinition> getComparisonExecuteSqlList() {
        return comparisonExecuteSqlList;
    }

    public void setComparisonExecuteSqlList(List<ExecuteSqlDefinition> comparisonExecuteSqlList) {
        this.comparisonExecuteSqlList = comparisonExecuteSqlList;
    }
}