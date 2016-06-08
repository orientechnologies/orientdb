package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public class IndexManagerTest extends DocumentDBBaseTest {
  private static final String CLASS_NAME = "classForIndexManagerTest";

  @Parameters(value = "url")
  public IndexManagerTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OSchema schema = database.getMetadata().getSchema();

    final OClass oClass = schema.createClass(CLASS_NAME);

    oClass.createProperty("fOne", OType.INTEGER);
    oClass.createProperty("fTwo", OType.STRING);
    oClass.createProperty("fThree", OType.BOOLEAN);
    oClass.createProperty("fFour", OType.INTEGER);

    oClass.createProperty("fSix", OType.STRING);
    oClass.createProperty("fSeven", OType.STRING);
  }

  @Test
  public void testCreateSimpleKeyInvalidNameIndex() {
    final OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();

    try {
      indexManager.createIndex("simple:key", OClass.INDEX_TYPE.UNIQUE.toString(), new OSimpleKeyIndexDefinition(-1, OType.INTEGER),
          null, null, null);
      fail();
    } catch (Exception e) {
      Throwable cause = e;
      while (cause.getCause() != null)
        cause = cause.getCause();

      assertTrue((cause instanceof IllegalArgumentException) || (cause instanceof OCommandSQLParsingException));
    }
  }

  @Test
  public void testCreateSimpleKeyIndexTest() {
    final OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();

    final OIndex result = indexManager.createIndex("simplekey", OClass.INDEX_TYPE.UNIQUE.toString(), new OSimpleKeyIndexDefinition(
        -1, OType.INTEGER), null, null, null);

    assertEquals(result.getName(), "simplekey");

    indexManager.reload();
    assertNull(database.getMetadata().getIndexManager().getClassIndex(CLASS_NAME, "simplekey"));
    assertEquals(database.getMetadata().getIndexManager().getIndex("simplekey").getName(), result.getName());
  }

  @Test
  public void testCreateNullKeyDefinitionIndexTest() {
    final OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();

    final OIndex result = indexManager.createIndex("nullkey", OClass.INDEX_TYPE.UNIQUE.toString(), null, null, null, null);

    assertEquals(result.getName(), "nullkey");
    indexManager.reload();

    assertNull(database.getMetadata().getIndexManager().getClassIndex(CLASS_NAME, "nullkey"));
    assertEquals(database.getMetadata().getIndexManager().getIndex("nullkey").getName(), result.getName());
  }

  @Test
  public void testCreateOnePropertyIndexTest() {
    final OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();

    final OIndex result = indexManager.createIndex("propertyone", OClass.INDEX_TYPE.UNIQUE.toString(),
        new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER), new int[] { database.getClusterIdByName(CLASS_NAME) },
        null, null);

    assertEquals(result.getName(), "propertyone");

    indexManager.reload();
    assertEquals(database.getMetadata().getIndexManager().getClassIndex(CLASS_NAME, "propertyone").getName(), result.getName());

  }

  @Test
  public void createCompositeIndexTestWithoutListener() {
    final OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();

    final OIndex result = indexManager.createIndex(
        "compositeone",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new OCompositeIndexDefinition(CLASS_NAME, Arrays.asList(
            new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER), new OPropertyIndexDefinition(CLASS_NAME, "fTwo",
                OType.STRING)

        ), -1), new int[] { database.getClusterIdByName(CLASS_NAME) }, null, null);

    assertEquals(result.getName(), "compositeone");

    indexManager.reload();
    assertEquals(database.getMetadata().getIndexManager().getClassIndex(CLASS_NAME, "compositeone").getName(), result.getName());
  }

  @Test
  public void createCompositeIndexTestWithListener() {
    final AtomicInteger atomicInteger = new AtomicInteger(0);
    final OProgressListener progressListener = new OProgressListener() {
      @Override
      public void onBegin(final Object iTask, final long iTotal, Object metadata) {
        atomicInteger.incrementAndGet();
      }

      @Override
      public boolean onProgress(final Object iTask, final long iCounter, final float iPercent) {
        return true;
      }

      @Override
      public void onCompletition(final Object iTask, final boolean iSucceed) {
        atomicInteger.incrementAndGet();
      }
    };

    final OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();

    final OIndex result = indexManager.createIndex(
        "compositetwo",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new OCompositeIndexDefinition(CLASS_NAME, Arrays.asList(
            new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER), new OPropertyIndexDefinition(CLASS_NAME, "fTwo",
                OType.STRING), new OPropertyIndexDefinition(CLASS_NAME, "fThree", OType.BOOLEAN)

        ), -1), new int[] { database.getClusterIdByName(CLASS_NAME) }, progressListener, null);

    assertEquals(result.getName(), "compositetwo");
    assertEquals(atomicInteger.get(), 2);

    indexManager.reload();
    assertEquals(database.getMetadata().getIndexManager().getClassIndex(CLASS_NAME, "compositetwo").getName(), result.getName());

  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedOneProperty() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fOne"));

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedDoesNotContainProperty() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fSix"));

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedTwoProperties() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne"));

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedThreeProperties() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedThreePropertiesBrokenFiledNameCase() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("ftwO", "Fone", "fThrEE"));

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedThreePropertiesBrokenClassNameCase() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed("ClaSSForIndeXManagerTeST", Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedPropertiesNotFirst() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fTree"));

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedPropertiesMoreThanNeeded() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedOnePropertyArrayParams() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fOne");

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedDoesNotContainPropertyArrayParams() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fSix");

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedTwoPropertiesArrayParams() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne");

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedThreePropertiesArrayParams() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne", "fThree");

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedPropertiesNotFirstArrayParams() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fTree");

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testAreIndexedPropertiesMoreThanNeededArrayParams() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne", "fThee", "fFour");

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesOnePropertyArrayParams() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, "fOne");

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "propertyone"));
    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesTwoPropertiesArrayParams() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, "fTwo", "fOne");
    assertEquals(result.size(), 2);

    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesThreePropertiesArrayParams() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, "fTwo", "fOne", "fThree");

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "compositetwo");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesNotInvolvedPropertiesArrayParams() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, "fTwo", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeededArrayParams() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, "fTwo", "fOne", "fThee", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetInvolvedIndexesPropertiesMorThanNeeded() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesNotExistingClass() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes("testlass", Arrays.asList("fOne"));

    assertTrue(result.isEmpty());
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesOneProperty() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, Arrays.asList("fOne"));

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "propertyone"));
    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesOnePropertyBrokenClassNameCase() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes("ClaSSforindeXmanagerTEST", Arrays.asList("fOne"));

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "propertyone"));
    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesTwoProperties() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, Arrays.asList("fTwo", "fOne"));
    assertEquals(result.size(), 2);

    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesThreeProperties() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "compositetwo");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesThreePropertiesBrokenFiledNameTest() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, Arrays.asList("ftwO", "foNe", "fThrEE"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "compositetwo");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesNotInvolvedProperties() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, Arrays.asList("fTwo", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeeded() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetClassInvolvedIndexesWithNullValues() {
    String className = "GetClassInvolvedIndexesWithNullValues";
    final OIndexManager indexManager = database.getMetadata().getIndexManager();
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.createClass(className);


    oClass.createProperty("one", OType.STRING);
    oClass.createProperty("two", OType.STRING);
    oClass.createProperty("three", OType.STRING);

    indexManager.createIndex(className+"_indexOne_notunique", OClass.INDEX_TYPE.NOTUNIQUE.toString(), new OPropertyIndexDefinition(className,
        "one", OType.STRING), oClass.getClusterIds(), null, null);

    indexManager.createIndex(
        className+"_indexOneTwo_notunique",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new OCompositeIndexDefinition(className, Arrays.asList(
            new OPropertyIndexDefinition(className, "one", OType.STRING),
            new OPropertyIndexDefinition(className, "two", OType.STRING)

        ), -1), oClass.getClusterIds(), null, null);

    indexManager.createIndex(
        className+"_indexOneTwoThree_notunique",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new OCompositeIndexDefinition(className, Arrays.asList(
            new OPropertyIndexDefinition(className, "one", OType.STRING),
            new OPropertyIndexDefinition(className, "two", OType.STRING),
            new OPropertyIndexDefinition(className, "three", OType.STRING)

        ), -1), oClass.getClusterIds(), null, null);


    Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(className, Arrays.asList("one"));
    assertEquals(result.size(), 3);

    result = indexManager.getClassInvolvedIndexes(className, Arrays.asList("one", "two"));
    assertEquals(result.size(), 2);

    result = indexManager.getClassInvolvedIndexes(className, Arrays.asList("one", "two", "three"));
    assertEquals(result.size(), 1);

    result = indexManager.getClassInvolvedIndexes(className, Arrays.asList("two"));
    assertEquals(result.size(), 0);

    result = indexManager.getClassInvolvedIndexes(className, Arrays.asList("two", "one", "three"));
    assertEquals(result.size(), 1);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassIndexes() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> indexes = indexManager.getClassIndexes(CLASS_NAME);
    final Set<OIndexDefinition> expectedIndexDefinitions = new HashSet<OIndexDefinition>();

    final OCompositeIndexDefinition compositeIndexOne = new OCompositeIndexDefinition(CLASS_NAME);

    compositeIndexOne.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER));
    compositeIndexOne.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING));
    compositeIndexOne.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexOne);

    final OCompositeIndexDefinition compositeIndexTwo = new OCompositeIndexDefinition(CLASS_NAME);

    compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER));
    compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING));
    compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fThree", OType.BOOLEAN));
    compositeIndexTwo.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexTwo);

    final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER);
    propertyIndex.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(propertyIndex);

    assertEquals(indexes.size(), 3);

    for (final OIndex index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }

  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassIndexesBrokenClassNameCase() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final Set<OIndex<?>> indexes = indexManager.getClassIndexes("ClassforindeXMaNAgerTeST");
    final Set<OIndexDefinition> expectedIndexDefinitions = new HashSet<OIndexDefinition>();

    final OCompositeIndexDefinition compositeIndexOne = new OCompositeIndexDefinition(CLASS_NAME);

    compositeIndexOne.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER));
    compositeIndexOne.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING));
    compositeIndexOne.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexOne);

    final OCompositeIndexDefinition compositeIndexTwo = new OCompositeIndexDefinition(CLASS_NAME);

    compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER));
    compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING));
    compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fThree", OType.BOOLEAN));
    compositeIndexTwo.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexTwo);

    final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER);
    propertyIndex.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(propertyIndex);

    assertEquals(indexes.size(), 3);

    for (final OIndex index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }

  }

  @Test
  public void testDropIndex() throws Exception {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    indexManager.createIndex("anotherproperty", OClass.INDEX_TYPE.UNIQUE.toString(), new OPropertyIndexDefinition(CLASS_NAME,
        "fOne", OType.INTEGER), new int[] { database.getClusterIdByName(CLASS_NAME) }, null, null);

    assertNotNull(indexManager.getIndex("anotherproperty"));
    assertNotNull(indexManager.getClassIndex(CLASS_NAME, "anotherproperty"));

    indexManager.dropIndex("anotherproperty");

    assertNull(indexManager.getIndex("anotherproperty"));
    assertNull(indexManager.getClassIndex(CLASS_NAME, "anotherproperty"));
  }

  @Test
  public void testDropSimpleKey() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();
    indexManager.createIndex("simplekeytwo", OClass.INDEX_TYPE.UNIQUE.toString(), new OSimpleKeyIndexDefinition(-1, OType.INTEGER),
        null, null, null);

    assertNotNull(indexManager.getIndex("simplekeytwo"));

    indexManager.dropIndex("simplekeytwo");

    assertNull(indexManager.getIndex("simplekeytwo"));
  }

  @Test
  public void testDropNullKeyDefinition() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    indexManager.createIndex("nullkeytwo", OClass.INDEX_TYPE.UNIQUE.toString(), null, null, null, null);

    assertNotNull(indexManager.getIndex("nullkeytwo"));

    indexManager.dropIndex("nullkeytwo");

    assertNull(indexManager.getIndex("nullkeytwo"));
  }

  @Test
  public void testDropAllClassIndexes() {
    final OClass oClass = database.getMetadata().getSchema().createClass("indexManagerTestClassTwo");
    oClass.createProperty("fOne", OType.INTEGER);

    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    indexManager.createIndex("twoclassproperty", OClass.INDEX_TYPE.UNIQUE.toString(), new OPropertyIndexDefinition(
            "indexManagerTestClassTwo", "fOne", OType.INTEGER),
        new int[] { database.getClusterIdByName("indexManagerTestClassTwo") }, null, null);

    assertFalse(indexManager.getClassIndexes("indexManagerTestClassTwo").isEmpty());

    indexManager.dropIndex("twoclassproperty");

    assertTrue(indexManager.getClassIndexes("indexManagerTestClassTwo").isEmpty());
  }

  @Test(dependsOnMethods = "testDropAllClassIndexes")
  public void testDropNonExistingClassIndex() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    indexManager.dropIndex("twoclassproperty");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassIndex() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final OIndex<?> result = indexManager.getClassIndex(CLASS_NAME, "propertyone");
    assertNotNull(result);
    assertEquals(result.getName(), "propertyone");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassIndexBrokenClassNameCase() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final OIndex<?> result = indexManager.getClassIndex("ClaSSforindeXManagerTeST", "propertyone");
    assertNotNull(result);
    assertEquals(result.getName(), "propertyone");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassIndexWrongIndexName() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final OIndex<?> result = indexManager.getClassIndex(CLASS_NAME, "propertyonetwo");
    assertNull(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest" })
  public void testGetClassIndexWrongClassName() {
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    final OIndex<?> result = indexManager.getClassIndex("testClassTT", "propertyone");
    assertNull(result);
  }

  private boolean containsIndex(final Collection<? extends OIndex> classIndexes, final String indexName) {
    for (final OIndex index : classIndexes) {
      if (index.getName().equals(indexName))
        return true;
    }
    return false;
  }
}
