package org.apache.dolphinscheduler.server.worker.task.dqs.rule;

import java.util.List;

public class FixedComparisonValueParameter {
  private List<RuleInputEntry> inputEntryList;

  public List<RuleInputEntry> getInputEntryList() {
    return inputEntryList;
  }

  public void setInputEntryList(List<RuleInputEntry> inputEntryList) {
    this.inputEntryList = inputEntryList;
  }
}