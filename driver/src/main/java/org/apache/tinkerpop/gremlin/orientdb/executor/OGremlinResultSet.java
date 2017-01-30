package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Map;
import java.util.Optional;

/**
 * Created by Enrico Risa on 24/01/17.
 */
public class OGremlinResultSet implements OResultSet {

    protected Traversal traversal;
    private OScriptTransformer transformer;
    private boolean closeGraph;

    public OGremlinResultSet(Traversal traversal, OScriptTransformer transformer) {
        this(traversal, transformer, false);
    }

    public OGremlinResultSet(Traversal traversal, OScriptTransformer transformer, boolean closeGraph) {
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

        Object next = traversal.next();
        return transformer.toResult(next);
    }

    @Override
    public void close() {
        try {
            traversal.close();
            if (closeGraph) {
                traversal.asAdmin().getGraph().ifPresent(graph -> {
                    try {
                        ((Graph) graph).close();
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
}
