package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.resultset.OResultSetMapper;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is intended for INSERT FROM SELECT. This step removes existing edge pointers so that the
 * resulting graph is still consistent
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class RemoveEdgePointersStep extends AbstractExecutionStep {

  private long cost = 0;

  public RemoveEdgePointersStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSetMapper(upstream, this::mapResult);
  }

  private OResult mapResult(OResult result) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {

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
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK AND EXCLUDE (possible) EXISTING EDGES ");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
