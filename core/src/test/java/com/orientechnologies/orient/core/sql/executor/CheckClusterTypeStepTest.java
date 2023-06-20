package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import org.junit.Assert;
import org.junit.Test;

/** Created by olena.kolesnyk on 30/07/2017. */
public class CheckClusterTypeStepTest extends TestUtilsFixture {

  private static final String CLASS_CLUSTER_NAME = "ClassClusterName";
  private static final String CLUSTER_NAME = "ClusterName";

  @Test
  public void shouldCheckClusterType() {
    OClass clazz = createClassInstance().addCluster(CLASS_CLUSTER_NAME);
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    CheckClusterTypeStep step =
        new CheckClusterTypeStep(CLASS_CLUSTER_NAME, clazz.getName(), context, false);

    OExecutionStream result = step.syncPull(context);
    Assert.assertEquals(0, result.stream(context).count());
  }

  @Test(expected = OCommandExecutionException.class)
  public void shouldThrowExceptionWhenClusterIsWrong() {
    db.addCluster(CLUSTER_NAME);
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    CheckClusterTypeStep step =
        new CheckClusterTypeStep(CLUSTER_NAME, createClassInstance().getName(), context, false);

    step.syncPull(context);
  }
}
