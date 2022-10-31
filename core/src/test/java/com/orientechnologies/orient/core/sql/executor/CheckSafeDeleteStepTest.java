package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CheckSafeDeleteStepTest extends TestUtilsFixture {

  private static final String VERTEX_CLASS_NAME = "VertexTestClass";
  private static final String EDGE_CLASS_NAME = "EdgeTestClass";

  private String className;

  public CheckSafeDeleteStepTest(String className) {
    this.className = className;
  }

  @Parameterized.Parameters(name = "Class name: {0}")
  public static Iterable<Object[]> documentTypes() {
    return Arrays.asList(
        new Object[][] {
          {VERTEX_CLASS_NAME}, {EDGE_CLASS_NAME},
        });
  }

  @Test(expected = OCommandExecutionException.class)
  public void shouldNotDeleteVertexAndEdge() {
    OCommandContext context = new OBasicCommandContext();
    switch (className) {
      case VERTEX_CLASS_NAME:
        database.createVertexClass(VERTEX_CLASS_NAME);
        break;
      case EDGE_CLASS_NAME:
        database.createEdgeClass(EDGE_CLASS_NAME);
        break;
    }

    CheckSafeDeleteStep step = new CheckSafeDeleteStep(context, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
            OInternalResultSet result = new OInternalResultSet();
            String simpleClassName = createClassInstance().getName();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                result.add(
                    new OResultInternal(new ODocument(i % 2 == 0 ? simpleClassName : className)));
              }
              done = true;
            }
            return result;
          }
        };

    step.setPrevious(previous);
    OResultSet result = step.syncPull(context, 20);
    while (result.hasNext()) {
      result.next();
    }
  }

  @Test
  public void shouldSafelyDeleteRecord() {
    OCommandContext context = new OBasicCommandContext();
    CheckSafeDeleteStep step = new CheckSafeDeleteStep(context, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
            OInternalResultSet result = new OInternalResultSet();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                result.add(new OResultInternal(new ODocument(createClassInstance().getName())));
              }
              done = true;
            }
            return result;
          }
        };

    step.setPrevious(previous);
    OResultSet result = step.syncPull(context, 10);
    Assert.assertEquals(10, result.stream().count());
    Assert.assertFalse(result.hasNext());
  }
}
