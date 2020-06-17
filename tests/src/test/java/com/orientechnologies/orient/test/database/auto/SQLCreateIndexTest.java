package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyListIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyRidBagIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class SQLCreateIndexTest extends DocumentDBBaseTest {

  private static final OType EXPECTED_PROP1_TYPE = OType.DOUBLE;
  private static final OType EXPECTED_PROP2_TYPE = OType.INTEGER;

  @Parameters(value = "url")
  public SQLCreateIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    if (database.isClosed()) database.open("admin", "admin");

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.createClass("sqlCreateIndexTestClass");
    oClass.createProperty("prop1", EXPECTED_PROP1_TYPE);
    oClass.createProperty("prop2", EXPECTED_PROP2_TYPE);
    oClass.createProperty("prop3", OType.EMBEDDEDMAP, OType.INTEGER);
    oClass.createProperty("prop5", OType.EMBEDDEDLIST, OType.INTEGER);
    oClass.createProperty("prop6", OType.EMBEDDEDLIST);
    oClass.createProperty("prop7", OType.EMBEDDEDMAP);
    oClass.createProperty("prop8", OType.INTEGER);
    oClass.createProperty("prop9", OType.LINKBAG);

    database.close();
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (database.isClosed()) database.open("admin", "admin");

    database.command(new OCommandSQL("delete from sqlCreateIndexTestClass")).execute();
    database.command(new OCommandSQL("drop class sqlCreateIndexTestClass")).execute();
    database.getMetadata().getSchema().reload();
    database.close();
  }

  @Test
  public void testOldSyntax() throws Exception {
    database
        .command(new OCommandSQL("CREATE INDEX sqlCreateIndexTestClass.prop1 UNIQUE"))
        .execute();

    database.getMetadata().getIndexManagerInternal().reload();
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexTestClass.prop1");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields().get(0), "prop1");
    Assert.assertEquals(indexDefinition.getTypes()[0], EXPECTED_PROP1_TYPE);
    Assert.assertEquals(index.getType(), "UNIQUE");
  }

  @Test
  public void testCreateCompositeIndex() throws Exception {
    database
        .command(
            new OCommandSQL(
                "CREATE INDEX sqlCreateIndexCompositeIndex ON sqlCreateIndexTestClass (prop1, prop2) UNIQUE"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexCompositeIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new OType[] {EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");
  }

  @Test
  public void testCreateEmbeddedMapIndex() throws Exception {
    database
        .command(
            new OCommandSQL(
                "CREATE INDEX sqlCreateIndexEmbeddedMapIndex ON sqlCreateIndexTestClass (prop3) UNIQUE"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexEmbeddedMapIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] {OType.STRING});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testOldStileCreateEmbeddedMapIndex() throws Exception {
    database
        .command(new OCommandSQL("CREATE INDEX sqlCreateIndexTestClass.prop3 UNIQUE"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexTestClass.prop3");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] {OType.STRING});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateEmbeddedMapWrongSpecifierIndexOne() throws Exception {
    try {
      database
          .command(
              new OCommandSQL(
                  "CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass (prop3 by ttt) UNIQUE"))
          .execute();
      Assert.fail();
    } catch (OCommandSQLParsingException e) {
    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexEmbeddedMapWrongSpecifierIndex");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  @Test
  public void testCreateEmbeddedMapWrongSpecifierIndexTwo() throws Exception {
    try {
      database
          .command(
              new OCommandSQL(
                  "CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass (prop3 b value) UNIQUE"))
          .execute();
      Assert.fail();
    } catch (OCommandSQLParsingException e) {

    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexEmbeddedMapWrongSpecifierIndex");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  @Test
  public void testCreateEmbeddedMapWrongSpecifierIndexThree() throws Exception {
    try {
      database
          .command(
              new OCommandSQL(
                  "CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass (prop3 by value t) UNIQUE"))
          .execute();
      Assert.fail();
    } catch (OCommandSQLParsingException e) {

    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexEmbeddedMapWrongSpecifierIndex");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  @Test
  public void testCreateEmbeddedMapByKeyIndex() throws Exception {
    database
        .command(
            new OCommandSQL(
                "CREATE INDEX sqlCreateIndexEmbeddedMapByKeyIndex ON sqlCreateIndexTestClass (prop3 by key) UNIQUE"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexEmbeddedMapByKeyIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] {OType.STRING});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateEmbeddedMapByValueIndex() throws Exception {
    database
        .command(
            new OCommandSQL(
                "CREATE INDEX sqlCreateIndexEmbeddedMapByValueIndex ON sqlCreateIndexTestClass (prop3 by value) UNIQUE"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexEmbeddedMapByValueIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] {OType.INTEGER});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.VALUE);
  }

  @Test
  public void testCreateEmbeddedListIndex() throws Exception {
    database
        .command(
            new OCommandSQL(
                "CREATE INDEX sqlCreateIndexEmbeddedListIndex ON sqlCreateIndexTestClass (prop5) NOTUNIQUE"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexEmbeddedListIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyListIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop5"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] {OType.INTEGER});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");
  }

  public void testCreateRidBagIndex() throws Exception {
    database
        .command(
            new OCommandSQL(
                "CREATE INDEX sqlCreateIndexRidBagIndex ON sqlCreateIndexTestClass (prop9) NOTUNIQUE"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexRidBagIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyRidBagIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop9"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] {OType.LINK});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");
  }

  public void testCreateOldStileEmbeddedListIndex() throws Exception {
    database
        .command(new OCommandSQL("CREATE INDEX sqlCreateIndexTestClass.prop5 NOTUNIQUE"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexTestClass.prop5");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyListIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop5"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] {OType.INTEGER});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");
  }

  public void testCreateOldStileRidBagIndex() throws Exception {
    database
        .command(new OCommandSQL("CREATE INDEX sqlCreateIndexTestClass.prop9 NOTUNIQUE"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexTestClass.prop9");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyRidBagIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop9"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] {OType.LINK});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");
  }

  @Test
  public void testCreateEmbeddedListWithoutLinkedTypeIndex() throws Exception {
    try {
      database
          .command(
              new OCommandSQL(
                  "CREATE INDEX sqlCreateIndexEmbeddedListWithoutLinkedTypeIndex ON sqlCreateIndexTestClass (prop6) UNIQUE"))
          .execute();
      Assert.fail();
    } catch (OIndexException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Linked type was not provided. "
                      + "You should provide linked type for embedded collections that are going to be indexed."));
    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexEmbeddedListWithoutLinkedTypeIndex");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  @Test
  public void testCreateEmbeddedMapWithoutLinkedTypeIndex() throws Exception {
    try {
      database
          .command(
              new OCommandSQL(
                  "CREATE INDEX sqlCreateIndexEmbeddedMapWithoutLinkedTypeIndex ON sqlCreateIndexTestClass (prop7 by value) UNIQUE"))
          .execute();
      Assert.fail();
    } catch (OIndexException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Linked type was not provided. "
                      + "You should provide linked type for embedded collections that are going to be indexed."));
    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexEmbeddedMapWithoutLinkedTypeIndex");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  @Test
  public void testCreateCompositeIndexWithTypes() throws Exception {
    final String query =
        new StringBuilder(
                "CREATE INDEX sqlCreateIndexCompositeIndex2 ON sqlCreateIndexTestClass (prop1, prop2) UNIQUE ")
            .append(EXPECTED_PROP1_TYPE)
            .append(", ")
            .append(EXPECTED_PROP2_TYPE)
            .toString();

    database.command(new OCommandSQL(query)).execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexCompositeIndex2");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new OType[] {EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");
  }

  @Test
  public void testCreateCompositeIndexWithWrongTypes() throws Exception {
    final String query =
        new StringBuilder(
                "CREATE INDEX sqlCreateIndexCompositeIndex3 ON sqlCreateIndexTestClass (prop1, prop2) UNIQUE ")
            .append(EXPECTED_PROP1_TYPE)
            .append(", ")
            .append(EXPECTED_PROP1_TYPE)
            .toString();

    try {
      database.command(new OCommandSQL(query)).execute();
      Assert.fail();
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Error on execution of command: sql.CREATE INDEX sqlCreateIndexCompositeIndex3 ON"));

      Throwable cause = e;
      while (cause.getCause() != null) cause = cause.getCause();

      Assert.assertEquals(cause.getClass(), IllegalArgumentException.class);
    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexCompositeIndex3");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  public void testCompositeIndexWithMetadata() {
    database
        .command(
            new OCommandSQL(
                "CREATE INDEX sqlCreateIndexCompositeIndexWithMetadata ON sqlCreateIndexTestClass (prop1, prop2) UNIQUE"
                    + " metadata {v1:23, v2:\"val2\"}"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexCompositeIndexWithMetadata");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new OType[] {EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");

    ODocument metadata = index.getMetadata();

    Assert.assertEquals(metadata.<Object>field("v1"), 23);
    Assert.assertEquals(metadata.field("v2"), "val2");
  }

  public void testOldIndexWithMetadata() {
    database
        .command(
            new OCommandSQL(
                "CREATE INDEX sqlCreateIndexTestClass.prop8 NOTUNIQUE  metadata {v1:23, v2:\"val2\"}"))
        .execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexTestClass.prop8");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop8"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] {OType.INTEGER});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");

    ODocument metadata = index.getMetadata();

    Assert.assertEquals(metadata.<Object>field("v1"), 23);
    Assert.assertEquals(metadata.field("v2"), "val2");
  }

  public void testCreateCompositeIndexWithTypesAndMetadata() throws Exception {
    final String query =
        new StringBuilder(
                "CREATE INDEX sqlCreateIndexCompositeIndex2WithConfig ON sqlCreateIndexTestClass (prop1, prop2) UNIQUE ")
            .append(EXPECTED_PROP1_TYPE)
            .append(", ")
            .append(EXPECTED_PROP2_TYPE)
            .append(" metadata {v1:23, v2:\"val2\"}")
            .toString();

    database.command(new OCommandSQL(query)).execute();
    database.getMetadata().getIndexManagerInternal().reload();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex("sqlCreateIndexCompositeIndex2WithConfig");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new OType[] {EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");

    ODocument metadata = index.getMetadata();
    Assert.assertEquals(metadata.<Object>field("v1"), 23);
    Assert.assertEquals(metadata.field("v2"), "val2");
  }
}
