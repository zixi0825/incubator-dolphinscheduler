package org.apache.dolphinscheduler.common.enums;

import com.fasterxml.jackson.annotation.JsonValue;

public enum InputType{
  /**
   *
   */
  DEFAULT("default"),
  STATISTICS("statistics"),
  COMPARISON("comparison");
 
  private String inputType;
  InputType(String inputType) {
      this.inputType = inputType;
  }
  
  @JsonValue
  public String getInputType() {
      return this.inputType;
  }
}