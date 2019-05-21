package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OQueryMetrics;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.Map;
import java.util.Optional;

/**
 * Created by Enrico Risa on 24/01/17.
 */
public class OGremlinScriptResultSet implements OResultSet, OQueryMetrics {

    private final String textTraversal;
    protected Traversal traversal;
    private OScriptTransformer transformer;
    private boolean closeGraph;
    private boolean closing = false;

    long startTime = 0;
    long totalExecutionTime = 0;

    boolean first  = true;

    public OGremlinScriptResultSet(String iText,Traversal traversal, OScriptTransformer transformer) {
        this(iText,traversal, transformer, false);
    }

    public OGremlinScriptResultSet(String iText,Traversal traversal, OScriptTransformer transformer, boolean closeGraph) {
        this.textTraversal = iText;
        this.traversal = traversal;
        this.transformer = transformer;
        this.closeGraph = closeGraph;
    }

    @Override
    public boolean hasNext() {
        return traversal.hasNext();
    }

    @Override
    public OResult next() {

        long begin = System.currentTimeMillis();

        if(first) {
            first = false;
            startTime = begin;
        }
        try {
          Object next = traversal.next();
          return transformer.toResult(next);
        } finally {
          totalExecutionTime += (System.currentTimeMillis() - begin);
        }
    }

    @Override
    public void close() {
        try {
            traversal.close();
            if (closeGraph) {
                traversal.asAdmin().getGraph().ifPresent(graph -> {
                    try {
                        OrientGraph g = (OrientGraph) graph;
                        if (!closing) {
                            closing = true;
                            g.close();
                        }
                    } catch (Exception e) {
                        throw OException.wrapException(new OCommandExecutionException("Error closing the Graph "), e);
                    }
                });
            }
        } catch (Exception e) {
            throw OException.wrapException(new OCommandExecutionException("Error closing the gremlin Result Set"), e);
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

  @Override public String getStatement() {
    return textTraversal;
  }

  @Override public long getStartTime() {
    return startTime;
  }

  @Override public long getElapsedTimeMillis() {
    return totalExecutionTime;
  }

  @Override public String getLanguage() {
    return "gremlin";
  }
}
