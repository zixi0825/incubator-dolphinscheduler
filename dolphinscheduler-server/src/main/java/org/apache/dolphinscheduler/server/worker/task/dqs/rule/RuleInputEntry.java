package org.apache.dolphinscheduler.server.worker.task.dqs.rule;

import org.apache.dolphinscheduler.common.enums.FormType;
import org.apache.dolphinscheduler.common.enums.InputType;
import org.apache.dolphinscheduler.common.enums.OptionSourceType;
import org.apache.dolphinscheduler.common.enums.ValueType;

public class RuleInputEntry{
  /**
   * form field name
   */
  private String field;
  /**
   * form type
    */
  private FormType type;
  /**
   * form title
   */
  private String title;
  /**
   * default value，can be null
   */
  private String value;
  /**
   * default options，can be null
   *  [{label:"",value:""}]
   */
  private String options;
  /**
   * ${field}
   */
  private String placeholder;
  /**
   * the source type of options，use default options or other
   */
  private OptionSourceType optionSourceType;
  /**
   * input entry type: string，array，number .etc
   */
  private ValueType valueType;
  /**
   * whether to display on the front end
   */
  private Boolean isShow;
  /**
   * input entry type: default,statistics,comparison
   */
  private InputType inputType;
  /**
   * whether to edit on the front end
   */
  private Boolean canEdit;

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public FormType getType() {
    return type;
  }

  public void setType(FormType type) {
    this.type = type;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getOptions() {
    return options;
  }

  public void setOptions(String options) {
    this.options = options;
  }

  public String getPlaceholder() {
    return placeholder;
  }

  public void setPlaceholder(String placeholder) {
    this.placeholder = placeholder;
  }

  public OptionSourceType getOptionSourceType() {
    return optionSourceType;
  }

  public void setOptionSourceType(OptionSourceType optionSourceType) {
    this.optionSourceType = optionSourceType;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public void setValueType(ValueType valueType) {
    this.valueType = valueType;
  }

  public Boolean getShow() {
    return isShow;
  }

  public void setShow(Boolean show) {
    isShow = show;
  }

  public InputType getInputType() {
    return inputType;
  }

  public void setInputType(InputType inputType) {
    this.inputType = inputType;
  }

  public Boolean getCanEdit() {
    return canEdit;
  }

  public void setCanEdit(Boolean canEdit) {
    this.canEdit = canEdit;
  }
}