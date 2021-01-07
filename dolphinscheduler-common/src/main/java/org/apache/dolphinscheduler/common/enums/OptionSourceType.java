package org.apache.dolphinscheduler.common.enums;

import com.fasterxml.jackson.annotation.JsonValue;

public enum OptionSourceType{

  /**
   *
   */
  DEFAULT("default"),
  DATASOURCE("datasource");
  
  private String optionSourceType;
  OptionSourceType(String optionSourceType) {
      this.optionSourceType = optionSourceType;
  }
  
  @JsonValue
  public String getOptionSourceType() {
      return this.optionSourceType;
  }
}