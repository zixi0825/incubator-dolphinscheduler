package org.apache.dolphinscheduler.common.enums;

import com.fasterxml.jackson.annotation.JsonValue;

public enum FormType{

  /**
   *
   */
  INPUT("input"),
  RADIO("radio"),
  SELECT("select"),
  CHECKBOX("checkbox"),
  CASCADER("cascader");
  
  private String formType;
  FormType(String formType) {
      this.formType = formType;
  }
  
  @JsonValue
  public String getFormType() {
      return this.formType;
  }
}