package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OUpdatedRecordsReturnHandler extends ORecordsReturnHandler {
  public OUpdatedRecordsReturnHandler(Object returnExpression, OCommandContext context) {
    super(returnExpression, context);
  }

  @Override
  protected ODocument preprocess(ODocument result) {
    return result;
  }

  @Override
  public void beforeUpdate(ODocument result) {

  }

  @Override
  public void afterUpdate(ODocument result) {
    storeResult(result);
  }
}
