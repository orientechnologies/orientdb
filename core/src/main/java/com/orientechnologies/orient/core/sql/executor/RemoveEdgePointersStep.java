package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.OElement;
import java.util.Map;
import java.util.Optional;
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
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public OResult next() {
        OResultInternal elem = (OResultInternal) upstream.next();
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {

          Set<String> propNames = elem.getPropertyNames();
          for (String propName :
              propNames.stream()
                  .filter(x -> x.startsWith("in_") || x.startsWith("out_"))
                  .collect(Collectors.toList())) {
            Object val = elem.getProperty(propName);
            if (val instanceof OElement) {
              if (((OElement) val).getSchemaType().map(x -> x.isSubClassOf("E")).orElse(false)) {
                elem.removeProperty(propName);
              }
            } else if (val instanceof Iterable) {
              for (Object o : (Iterable) val) {
                if (o instanceof OElement) {
                  if (((OElement) o).getSchemaType().map(x -> x.isSubClassOf("E")).orElse(false)) {
                    elem.removeProperty(propName);
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
        return elem;
      }

      @Override
      public void close() {
        upstream.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
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
