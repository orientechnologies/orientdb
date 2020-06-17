package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import org.junit.Assert;
import org.junit.Test;

/** Created by olena.kolesnyk on 28/07/2017. */
public class CountStepTest {

  private static final String PROPERTY_NAME = "testPropertyName";
  private static final String PROPERTY_VALUE = "testPropertyValue";
  private static final String COUNT_PROPERTY_NAME = "count";

  @Test
  public void shouldCountRecords() {
    OCommandContext context = new OBasicCommandContext();
    CountStep step = new CountStep(context, false);

    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
            OInternalResultSet result = new OInternalResultSet();
            if (!done) {
              for (int i = 0; i < 100; i++) {
                OResultInternal item = new OResultInternal();
                item.setProperty(PROPERTY_NAME, PROPERTY_VALUE);
                result.add(item);
              }
              done = true;
            }
            return result;
          }
        };

    step.setPrevious(previous);
    OResultSet result = step.syncPull(context, 100);
    Assert.assertEquals(100, (long) result.next().getProperty(COUNT_PROPERTY_NAME));
    Assert.assertFalse(result.hasNext());
  }
}
