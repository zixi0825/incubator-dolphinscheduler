package org.apache.dolphinscheduler.server.worker.task.dqs.rule;

public class ExecuteSqlDefinition{
  /**
   * index，ensure the execution order of sql
   */
  private int index;
  /**
   * SQL Statement
   */
  private String sql;
  /**
   * table alias name
   */
  private String tableAlias;
  /**
   *  is Middle sql
   */
  private boolean isMid;

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getTableAlias() {
    return tableAlias;
  }

  public void setTableAlias(String tableAlias) {
    this.tableAlias = tableAlias;
  }

  public boolean isMid() {
    return isMid;
  }

  public void setMid(boolean mid) {
    isMid = mid;
  }
}