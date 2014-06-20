package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * 
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class ORecordCountHandler implements OReturnHandler {
  private int count = 0;

  @Override
  public void reset() {
    count = 0;
  }

  @Override
  public void beforeUpdate(ODocument result) {
  }

  @Override
  public void afterUpdate(ODocument result) {
    count++;
  }

  @Override
  public Object ret() {
    return count;
  }
}
