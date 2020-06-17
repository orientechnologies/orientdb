package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;
import com.orientechnologies.orient.core.sql.parser.OIndexName;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Created by olena.kolesnyk on 28/07/2017. */
@RunWith(Parameterized.class)
public class CountFromIndexStepTest extends TestUtilsFixture {

  private static final String PROPERTY_NAME = "testPropertyName";
  private static final String PROPERTY_VALUE = "testPropertyValue";
  private static final String ALIAS = "size";
  private static String indexName;

  private OIndexIdentifier.Type identifierType;

  public CountFromIndexStepTest(OIndexIdentifier.Type identifierType) {
    this.identifierType = identifierType;
  }

  @Parameterized.Parameters(name = "OIndexIdentifier.Type: {0}")
  public static Iterable<Object[]> types() {
    return Arrays.asList(
        new Object[][] {
          {OIndexIdentifier.Type.INDEX},
          {OIndexIdentifier.Type.VALUES},
          {OIndexIdentifier.Type.VALUESASC},
          {OIndexIdentifier.Type.VALUESDESC},
        });
  }

  @BeforeClass
  public static void precondition() {
    OClass clazz = createClassInstance();
    clazz.createProperty(PROPERTY_NAME, OType.STRING);
    String className = clazz.getName();
    indexName = className + "." + PROPERTY_NAME;
    clazz.createIndex(indexName, OClass.INDEX_TYPE.NOTUNIQUE, PROPERTY_NAME);

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument(className);
      document.field(PROPERTY_NAME, PROPERTY_VALUE);
      document.save();
    }
  }

  @Test
  public void shouldCountRecordsOfIndex() {
    OIndexName name = new OIndexName(-1);
    name.setValue(indexName);
    OIndexIdentifier identifier = new OIndexIdentifier(-1);
    identifier.setIndexName(name);
    identifier.setIndexNameString(name.getValue());
    identifier.setType(identifierType);

    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(database);
    CountFromIndexStep step = new CountFromIndexStep(identifier, ALIAS, context, false);

    OResultSet result = step.syncPull(context, 20);
    Assert.assertEquals(20, (long) result.next().getProperty(ALIAS));
    Assert.assertFalse(result.hasNext());
  }
}
