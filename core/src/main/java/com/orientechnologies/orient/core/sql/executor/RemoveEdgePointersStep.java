package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is intended for INSERT FROM SELECT. This step removes existing edge pointers so that the
 * resulting graph is still consistent
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class RemoveEdgePointersStep extends AbstractExecutionStep {

  public RemoveEdgePointersStep() {
    super();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    Set<String> propNames = result.getPropertyNames();
    for (String propName :
        propNames.stream()
            .filter(x -> x.startsWith("in_") || x.startsWith("out_"))
            .collect(Collectors.toList())) {
      Object val = result.getProperty(propName);
      if (val instanceof OElement) {
        if (((OElement) val).getSchemaType().map(x -> x.isSubClassOf("E")).orElse(false)) {
          ((OResultInternal) result).removeProperty(propName);
        }
      } else if (val instanceof Iterable) {
        for (Object o : (Iterable) val) {
          if (o instanceof OElement) {
            if (((OElement) o).getSchemaType().map(x -> x.isSubClassOf("E")).orElse(false)) {
              ((OResultInternal) result).removeProperty(propName);
              break;
            }
          }
        }
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK AND EXCLUDE (possible) EXISTING EDGES ");
    if (ctx.isProfilingEnabled()) {
      result.append(" (" + ctx.getCostFormatted(this) + ")");
    }
    return result.toString();
  }
}
