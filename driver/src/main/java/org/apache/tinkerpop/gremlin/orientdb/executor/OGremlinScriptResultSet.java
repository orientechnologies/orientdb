package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OQueryMetrics;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.Optional;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;

/** Created by Enrico Risa on 24/01/17. */
public class OGremlinScriptResultSet implements OResultSet, OQueryMetrics {

  private final String textTraversal;
  protected Traversal traversal;
  private OScriptTransformer transformer;
  private boolean closeGraph;
  private boolean closing = false;

  long startTime;
  long totalExecutionTime = 0;

  public OGremlinScriptResultSet(
      String iText, Traversal traversal, OScriptTransformer transformer) {
    this(iText, traversal, transformer, false);
  }

  public OGremlinScriptResultSet(
      String iText, Traversal traversal, OScriptTransformer transformer, boolean closeGraph) {
    this.textTraversal = iText;
    this.traversal = traversal;
    this.transformer = transformer;
    this.closeGraph = closeGraph;
    this.startTime = System.currentTimeMillis();
  }

  @Override
  public boolean hasNext() {
    try {
      return traversal.hasNext();
    } catch (TraversalInterruptedException e) {
      throw new OInterruptedException("Timeout expired");
    } catch (OStorageException se) {
      if (se.getCause() instanceof TraversalInterruptedException
          || se.getCause() instanceof ClosedChannelException) {
        throw new OInterruptedException("Timeout expired");
      }
      throw se;
    }
  }

  @Override
  public OResult next() {

    long begin = System.currentTimeMillis();

    try {
      Object next = traversal.next();
      return transformer.toResult(next);
    } catch (TraversalInterruptedException e) {
      throw new OInterruptedException("Timeout expired");
    } catch (OStorageException se) {
      if (se.getCause() instanceof TraversalInterruptedException
          || se.getCause() instanceof ClosedChannelException) {
        throw new OInterruptedException("Timeout expired");
      }
      throw se;
    } finally {
      totalExecutionTime += (System.currentTimeMillis() - begin);
    }
  }

  @Override
  public void close() {
    try {
      traversal.close();
      if (closeGraph) {
        traversal
            .asAdmin()
            .getGraph()
            .ifPresent(
                graph -> {
                  try {
                    OrientGraph g = (OrientGraph) graph;
                    if (!closing) {
                      closing = true;
                      g.close();
                    }
                  } catch (Exception e) {
                    throw OException.wrapException(
                        new OCommandExecutionException("Error closing the Graph "), e);
                  }
                });
      }
    } catch (Exception e) {
      throw OException.wrapException(
          new OCommandExecutionException("Error closing the gremlin Result Set"), e);
    }
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return null;
  }

  @Override
  public String getStatement() {
    return textTraversal;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public long getElapsedTimeMillis() {
    return totalExecutionTime;
  }

  @Override
  public String getLanguage() {
    return "gremlin";
  }
}
