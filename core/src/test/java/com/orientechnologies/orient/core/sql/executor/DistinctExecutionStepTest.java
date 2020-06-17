package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import org.junit.Assert;
import org.junit.Test;

/** Created by luigidellaquila on 26/07/16. */
public class DistinctExecutionStepTest {

  @Test
  public void test() {
    OCommandContext ctx = new OBasicCommandContext();
    DistinctExecutionStep step = new DistinctExecutionStep(ctx, false);

    AbstractExecutionStep prev =
        new AbstractExecutionStep(ctx, false) {
          boolean done = false;

          @Override
          public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
            OInternalResultSet result = new OInternalResultSet();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                OResultInternal item = new OResultInternal();
                item.setProperty("name", i % 2 == 0 ? "foo" : "bar");
                result.add(item);
              }
              done = true;
            }
            return result;
          }
        };

    step.setPrevious(prev);
    OResultSet res = step.syncPull(ctx, 10);
    Assert.assertTrue(res.hasNext());
    res.next();
    Assert.assertTrue(res.hasNext());
    res.next();
    Assert.assertFalse(res.hasNext());
  }
}
