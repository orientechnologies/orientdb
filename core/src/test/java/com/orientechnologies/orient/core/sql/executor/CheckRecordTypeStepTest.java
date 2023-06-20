package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** Created by olena.kolesnyk on 31/07/2017. */
public class CheckRecordTypeStepTest extends TestUtilsFixture {

  @Test
  public void shouldCheckRecordsOfOneType() {
    OCommandContext context = new OBasicCommandContext();
    String className = createClassInstance().getName();
    CheckRecordTypeStep step = new CheckRecordTypeStep(context, className, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
            List<OResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                result.add(new OResultInternal(new ODocument(className)));
              }
              done = true;
            }
            return OExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    OExecutionStream result = step.syncPull(context);
    Assert.assertEquals(10, result.stream(context).count());
    Assert.assertFalse(result.hasNext(context));
  }

  @Test
  public void shouldCheckRecordsOfSubclasses() {
    OCommandContext context = new OBasicCommandContext();
    OClass parentClass = createClassInstance();
    OClass childClass = createChildClassInstance(parentClass);
    CheckRecordTypeStep step = new CheckRecordTypeStep(context, parentClass.getName(), false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
            List<OResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                result.add(
                    new OResultInternal(new ODocument(i % 2 == 0 ? parentClass : childClass)));
              }
              done = true;
            }
            return OExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    OExecutionStream result = step.syncPull(context);
    Assert.assertEquals(10, result.stream(context).count());
    Assert.assertFalse(result.hasNext(context));
  }

  @Test(expected = OCommandExecutionException.class)
  public void shouldThrowExceptionWhenTypeIsDifferent() {
    OCommandContext context = new OBasicCommandContext();
    String firstClassName = createClassInstance().getName();
    String secondClassName = createClassInstance().getName();
    CheckRecordTypeStep step = new CheckRecordTypeStep(context, firstClassName, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
            List<OResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                result.add(
                    new OResultInternal(
                        new ODocument(i % 2 == 0 ? firstClassName : secondClassName)));
              }
              done = true;
            }
            return OExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    OExecutionStream result = step.syncPull(context);
    while (result.hasNext(context)) {
      result.next(context);
    }
  }
}
