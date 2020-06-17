package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class SchemaIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SchemaIndexTest(@Optional final String iURL) {
    super(iURL);
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass superTest = schema.createClass("SchemaSharedIndexSuperTest");
    final OClass test = schema.createClass("SchemaIndexTest", superTest);
    test.createProperty("prop1", OType.DOUBLE);
    test.createProperty("prop2", OType.DOUBLE);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    database.command(new OCommandSQL("drop class SchemaIndexTest")).execute();
    database.command(new OCommandSQL("drop class SchemaSharedIndexSuperTest")).execute();
    database.getMetadata().getSchema().reload();
  }

  @Test
  public void testDropClass() throws Exception {
    database
        .command(
            new OCommandSQL(
                "CREATE INDEX SchemaSharedIndexCompositeIndex ON SchemaIndexTest (prop1, prop2) UNIQUE"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();
    Assert.assertNotNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "SchemaSharedIndexCompositeIndex"));

    database.getMetadata().getSchema().dropClass("SchemaIndexTest");
    database.getMetadata().getSchema().reload();
    database.getMetadata().getIndexManagerInternal().reload();

    Assert.assertNull(database.getMetadata().getSchema().getClass("SchemaIndexTest"));
    Assert.assertNotNull(database.getMetadata().getSchema().getClass("SchemaSharedIndexSuperTest"));

    Assert.assertNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "SchemaSharedIndexCompositeIndex"));
  }

  @Test
  public void testDropSuperClass() throws Exception {
    database
        .command(
            new OCommandSQL(
                "CREATE INDEX SchemaSharedIndexCompositeIndex ON SchemaIndexTest (prop1, prop2) UNIQUE"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    try {
      database.getMetadata().getSchema().dropClass("SchemaSharedIndexSuperTest");
      Assert.fail();
    } catch (OSchemaException e) {
      Assert.assertTrue(
          e.getMessage()
              .startsWith(
                  "Class 'SchemaSharedIndexSuperTest' cannot be dropped because it has sub classes"));
    }

    database.getMetadata().getSchema().reload();

    Assert.assertNotNull(database.getMetadata().getSchema().getClass("SchemaIndexTest"));
    Assert.assertNotNull(database.getMetadata().getSchema().getClass("SchemaSharedIndexSuperTest"));

    Assert.assertNotNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "SchemaSharedIndexCompositeIndex"));
  }

  public void testPolymorphicIdsPropagationAfterClusterAddRemove() {
    final OSchema schema = database.getMetadata().getSchema();

    OClass polymorpicIdsPropagationSuperSuper =
        schema.getClass("polymorpicIdsPropagationSuperSuper");

    if (polymorpicIdsPropagationSuperSuper == null)
      polymorpicIdsPropagationSuperSuper = schema.createClass("polymorpicIdsPropagationSuperSuper");

    OClass polymorpicIdsPropagationSuper = schema.getClass("polymorpicIdsPropagationSuper");
    if (polymorpicIdsPropagationSuper == null)
      polymorpicIdsPropagationSuper = schema.createClass("polymorpicIdsPropagationSuper");

    OClass polymorpicIdsPropagation = schema.getClass("polymorpicIdsPropagation");
    if (polymorpicIdsPropagation == null)
      polymorpicIdsPropagation = schema.createClass("polymorpicIdsPropagation");

    polymorpicIdsPropagation.setSuperClass(polymorpicIdsPropagationSuper);
    polymorpicIdsPropagationSuper.setSuperClass(polymorpicIdsPropagationSuperSuper);

    polymorpicIdsPropagationSuperSuper.createProperty("value", OType.STRING);
    polymorpicIdsPropagationSuperSuper.createIndex(
        "PolymorpicIdsPropagationSuperSuperIndex", OClass.INDEX_TYPE.UNIQUE, "value");

    int counter = 0;

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("polymorpicIdsPropagation");
      document.field("value", "val" + counter);
      document.save();

      counter++;
    }

    final int clusterId2 = database.addCluster("polymorpicIdsPropagationSuperSuper2");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument();
      document.field("value", "val" + counter);
      document.save("polymorpicIdsPropagationSuperSuper2");

      counter++;
    }

    polymorpicIdsPropagation.addCluster("polymorpicIdsPropagationSuperSuper2");

    assertContains(polymorpicIdsPropagationSuperSuper.getPolymorphicClusterIds(), clusterId2);
    assertContains(polymorpicIdsPropagationSuper.getPolymorphicClusterIds(), clusterId2);

    List<ODocument> result =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select from polymorpicIdsPropagationSuperSuper where value = 'val12'"));

    Assert.assertEquals(result.size(), 1);

    ODocument doc = result.get(0);
    Assert.assertEquals(doc.getSchemaClass().getName(), "polymorpicIdsPropagation");

    polymorpicIdsPropagation.removeClusterId(clusterId2);

    assertDoesNotContain(polymorpicIdsPropagationSuperSuper.getPolymorphicClusterIds(), clusterId2);
    assertDoesNotContain(polymorpicIdsPropagationSuper.getPolymorphicClusterIds(), clusterId2);

    result =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select from polymorpicIdsPropagationSuperSuper  where value = 'val12'"));
    Assert.assertTrue(result.isEmpty());
  }

  public void testIndexWithNumberProperties() {
    OClass oclass = database.getMetadata().getSchema().createClass("SchemaIndexTest_numberclass");
    oclass.createProperty("1", OType.STRING).setMandatory(false);
    oclass.createProperty("2", OType.STRING).setMandatory(false);
    oclass.createIndex("SchemaIndexTest_numberclass_1_2", OClass.INDEX_TYPE.UNIQUE, "1", "2");

    database.getMetadata().getSchema().dropClass(oclass.getName());
  }

  private void assertContains(int[] clusterIds, int clusterId) {
    boolean contains = false;
    for (int cluster : clusterIds) {
      if (cluster == clusterId) {
        contains = true;
        break;
      }
    }

    Assert.assertTrue(contains);
  }

  private void assertDoesNotContain(int[] clusterIds, int clusterId) {
    boolean contains = false;
    for (int cluster : clusterIds) {
      if (cluster == clusterId) {
        contains = true;
        break;
      }
    }

    Assert.assertTrue(!contains);
  }
}
