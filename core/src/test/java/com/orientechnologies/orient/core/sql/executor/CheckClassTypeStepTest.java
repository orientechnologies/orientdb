package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import org.junit.Assert;
import org.junit.Test;

/** Created by olena.kolesnyk on 30/07/2017. */
public class CheckClassTypeStepTest extends TestUtilsFixture {

  @Test
  public void shouldCheckSubclasses() {
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(database);
    OClass parentClass = createClassInstance();
    OClass childClass = createChildClassInstance(parentClass);
    CheckClassTypeStep step =
        new CheckClassTypeStep(childClass.getName(), parentClass.getName(), context, false);

    OResultSet result = step.syncPull(context, 20);
    Assert.assertEquals(0, result.stream().count());
  }

  @Test
  public void shouldCheckOneType() {
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(database);
    String className = createClassInstance().getName();
    CheckClassTypeStep step = new CheckClassTypeStep(className, className, context, false);

    OResultSet result = step.syncPull(context, 20);
    Assert.assertEquals(0, result.stream().count());
  }

  @Test(expected = OCommandExecutionException.class)
  public void shouldThrowExceptionWhenClassIsNotParent() {
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(database);
    CheckClassTypeStep step =
        new CheckClassTypeStep(
            createClassInstance().getName(), createClassInstance().getName(), context, false);

    step.syncPull(context, 20);
  }
}
