package com.orientechnologies.orient.core.sql;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public abstract class ORecordsReturnHandler implements OReturnHandler {
  private List<Object>          results;
  private final Object          returnExpression;
  private final OCommandContext context;

  protected ORecordsReturnHandler(Object returnExpression, OCommandContext context) {
    this.returnExpression = returnExpression;
    this.context = context;
  }

  @Override
  public void reset() {
    results = new ArrayList<Object>();
  }

  protected void storeResult(ODocument result) {
    final ODocument processedResult = preprocess(result);

    results.add(evaluateExpression(processedResult));
  }

  private Object evaluateExpression(ODocument record) {
    if (returnExpression == null) {
      return record;
    } else {
      final Object itemResult;
      final ODocument wrappingDoc;
      context.setVariable("current", record);
      itemResult = OSQLHelper.getValue(returnExpression, (ODocument) ((OIdentifiable) record).getRecord(), context);
      if (itemResult instanceof OIdentifiable)
        return itemResult;
      else {
        // WRAP WITH ODOCUMENT IF NEEDED
        wrappingDoc = new ODocument("result", itemResult);
        wrappingDoc.field("rid", record.getIdentity());// passing record id.In many cases usable on client side
        wrappingDoc.field("version", record.getVersion());
        return wrappingDoc;
      }
    }
  }

  @Override
  public Object ret() {
    return results;
  }

  protected abstract ODocument preprocess(ODocument result);
}
