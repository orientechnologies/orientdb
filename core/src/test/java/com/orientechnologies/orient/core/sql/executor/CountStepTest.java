package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.ArrayList;
import java.util.List;
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
    CountStep step = new CountStep();

    AbstractExecutionStep previous =
        new AbstractExecutionStep() {
          boolean done = false;

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
            List<OResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 100; i++) {
                OResultInternal item = new OResultInternal();
                item.setProperty(PROPERTY_NAME, PROPERTY_VALUE);
                result.add(item);
              }
              done = true;
            }
            return OExecutionStream.resultCollection(result);
          }
        };

    step.setPrevious(previous);
    OExecutionStream result = step.start(context);
    Assert.assertEquals(100, (long) result.next(context).getProperty(COUNT_PROPERTY_NAME));
    Assert.assertFalse(result.hasNext(context));
  }
}
