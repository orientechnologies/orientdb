package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public abstract class ORecordsReturnHandler implements OReturnHandler {
  private final Object          returnExpression;
  private final OCommandContext context;
  private List<Object>          results;

  protected ORecordsReturnHandler(final Object returnExpression, final OCommandContext context) {
    this.returnExpression = returnExpression;
    this.context = context;
  }

  @Override
  public void reset() {
    results = new ArrayList<Object>();
  }

  @Override
  public Object ret() {
    return results;
  }

  protected void storeResult(final ODocument result) {
    final ODocument processedResult = preprocess(result);

    results.add(evaluateExpression(processedResult));
  }

  protected abstract ODocument preprocess(final ODocument result);

  private Object evaluateExpression(final ODocument record) {
    if (returnExpression == null) {
      return record;
    } else {
      final Object itemResult;
      final ODocument wrappingDoc;
      context.setVariable("current", record);

      itemResult = OSQLHelper.getValue(returnExpression, (ODocument) ((OIdentifiable) record).getRecord(), context);
      if (itemResult instanceof OIdentifiable)
        return itemResult;

      // WRAP WITH ODOCUMENT TO BE TRANSFERRED THROUGH BINARY DRIVER
      return new ODocument("value", itemResult);
    }
  }
}
