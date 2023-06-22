package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import org.junit.Assert;
import org.junit.Test;

/** Created by olena.kolesnyk on 30/07/2017. */
public class CheckClassTypeStepTest extends TestUtilsFixture {

  @Test
  public void shouldCheckSubclasses() {
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    OClass parentClass = createClassInstance();
    OClass childClass = createChildClassInstance(parentClass);
    CheckClassTypeStep step =
        new CheckClassTypeStep(childClass.getName(), parentClass.getName(), context, false);

    OExecutionStream result = step.start(context);
    Assert.assertEquals(0, result.stream(context).count());
  }

  @Test
  public void shouldCheckOneType() {
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    String className = createClassInstance().getName();
    CheckClassTypeStep step = new CheckClassTypeStep(className, className, context, false);

    OExecutionStream result = step.start(context);
    Assert.assertEquals(0, result.stream(context).count());
  }

  @Test(expected = OCommandExecutionException.class)
  public void shouldThrowExceptionWhenClassIsNotParent() {
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    CheckClassTypeStep step =
        new CheckClassTypeStep(
            createClassInstance().getName(), createClassInstance().getName(), context, false);

    step.start(context);
  }
}
