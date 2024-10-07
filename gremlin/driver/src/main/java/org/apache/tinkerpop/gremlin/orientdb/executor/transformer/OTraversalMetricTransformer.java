package org.apache.tinkerpop.gremlin.orientdb.executor.transformer;

import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.ImmutableMetrics;

/** Created by Enrico Risa on 26/05/2017. */
public class OTraversalMetricTransformer implements OResultTransformer<DefaultTraversalMetrics> {
  @Override
  public OResult transform(DefaultTraversalMetrics value) {

    OResultInternal result = new OResultInternal();
    result.setProperty("time (ms)", value.getDuration(TimeUnit.MILLISECONDS));

    List<OResultInternal> steps =
        value.getMetrics().stream().map(this::mapMetric).collect(Collectors.toList());

    result.setProperty("steps", steps);
    return result;
  }

  private OResultInternal mapMetric(ImmutableMetrics m) {
    OResultInternal internal = new OResultInternal();
    internal.setProperty("id", m.getId());
    internal.setProperty("time (ms)", m.getDuration(TimeUnit.MILLISECONDS));
    internal.setProperty("name", m.getName());
    internal.setProperty("counts", m.getCounts());
    internal.setProperty(
        "nested", m.getNested().stream().map(this::mapMetric).collect(Collectors.toList()));
    return internal;
  }
}
