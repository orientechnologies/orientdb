package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import org.junit.Assert;
import org.junit.Test;

/** Created by olena.kolesnyk on 28/07/2017. */
public class CountFromClassStepTest extends TestUtilsFixture {

  private static final String ALIAS = "size";

  @Test
  public void shouldCountRecordsOfClass() {
    String className = createClassInstance().getName();
    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument(className);
      document.save();
    }

    OIdentifier classIdentifier = new OIdentifier(className);

    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(database);
    CountFromClassStep step = new CountFromClassStep(classIdentifier, ALIAS, context, false);

    OResultSet result = step.syncPull(context, 20);
    Assert.assertEquals(20, (long) result.next().getProperty(ALIAS));
    Assert.assertFalse(result.hasNext());
  }
}
