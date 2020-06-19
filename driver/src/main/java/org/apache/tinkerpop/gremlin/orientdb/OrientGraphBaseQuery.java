package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import java.util.Optional;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;

public interface OrientGraphBaseQuery {

  OGremlinResultSet execute(OGraph graph);

  Optional<OExecutionPlan> explain(OGraph graph);

  int usedIndexes(OGraph graph);
}
