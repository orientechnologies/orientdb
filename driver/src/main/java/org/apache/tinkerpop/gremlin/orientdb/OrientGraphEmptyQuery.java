package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Optional;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;

/** Created by Enrico Risa on 05/01/2018. */
public class OrientGraphEmptyQuery implements OrientGraphBaseQuery {
  @Override
  public OGremlinResultSet execute(OGraph graph) {
    return new OGremlinResultSet(
        null, new OExecutionResultSet(OExecutionStream.empty(), null, null));
  }

  @Override
  public Optional<OExecutionPlan> explain(OGraph graph) {
    return Optional.empty();
  }

  @Override
  public int usedIndexes(OGraph graph) {
    return 0;
  }
}
