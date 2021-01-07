package org.apache.dolphinscheduler.common.enums;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ValueType{
  STRING("string"),
  LIST("list"),
  NUMBER("number");
  
  private String valueType;
  ValueType(String valueType) {
      this.valueType = valueType;
  }
  
  @JsonValue
  public String getValueType() {
      return this.valueType;
  }
}