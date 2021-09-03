package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

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
  public void testCreateOnePropertyIndexTest() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final OIndex result =
        indexManager.createIndex(
            database,
            "propertyone",
            OClass.INDEX_TYPE.UNIQUE.toString(),
            new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER),
            new int[] {database.getClusterIdByName(CLASS_NAME)},
            null,
            null);

    assertEquals(result.getName(), "propertyone");

    indexManager.reload();
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, CLASS_NAME, "propertyone")
            .getName(),
        result.getName());
  }

  @Test
  public void createCompositeIndexTestWithoutListener() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final OIndex result =
        indexManager.createIndex(
            database,
            "compositeone",
            OClass.INDEX_TYPE.NOTUNIQUE.toString(),
            new OCompositeIndexDefinition(
                CLASS_NAME,
                Arrays.asList(
                    new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER),
                    new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING)),
                -1),
            new int[] {database.getClusterIdByName(CLASS_NAME)},
            null,
            null);

    assertEquals(result.getName(), "compositeone");

    indexManager.reload();
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, CLASS_NAME, "compositeone")
            .getName(),
        result.getName());
  }

  @Test
  public void createCompositeIndexTestWithListener() {
    final AtomicInteger atomicInteger = new AtomicInteger(0);
    final OProgressListener progressListener =
        new OProgressListener() {
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

    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final OIndex result =
        indexManager.createIndex(
            database,
            "compositetwo",
            OClass.INDEX_TYPE.NOTUNIQUE.toString(),
            new OCompositeIndexDefinition(
                CLASS_NAME,
                Arrays.asList(
                    new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER),
                    new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING),
                    new OPropertyIndexDefinition(CLASS_NAME, "fThree", OType.BOOLEAN)),
                -1),
            new int[] {database.getClusterIdByName(CLASS_NAME)},
            progressListener,
            null);

    assertEquals(result.getName(), "compositetwo");
    assertEquals(atomicInteger.get(), 2);

    indexManager.reload();
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, CLASS_NAME, "compositetwo")
            .getName(),
        result.getName());
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedOneProperty() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fOne"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedDoesNotContainProperty() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fSix"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedTwoProperties() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreeProperties() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result =
        indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreePropertiesBrokenFiledNameCase() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result =
        indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreePropertiesBrokenClassNameCase() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result =
        indexManager.areIndexed(
            "ClaSSForIndeXManagerTeST", Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesNotFirst() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fTree"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesMoreThanNeeded() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result =
        indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedOnePropertyArrayParams() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fOne");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedDoesNotContainPropertyArrayParams() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fSix");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedTwoPropertiesArrayParams() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreePropertiesArrayParams() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne", "fThree");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesNotFirstArrayParams() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fTree");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesMoreThanNeededArrayParams() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne", "fThee", "fFour");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesOnePropertyArrayParams() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result = indexManager.getClassInvolvedIndexes(database, CLASS_NAME, "fOne");

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "propertyone"));
    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesTwoPropertiesArrayParams() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, "fTwo", "fOne");
    assertEquals(result.size(), 2);

    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesThreePropertiesArrayParams() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, "fTwo", "fOne", "fThree");

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "compositetwo");
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesNotInvolvedPropertiesArrayParams() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, "fTwo", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeededArrayParams() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(
            database, CLASS_NAME, "fTwo", "fOne", "fThee", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetInvolvedIndexesPropertiesMorThanNeeded() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(
            database, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesNotExistingClass() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(database, "testlass", Arrays.asList("fOne"));

    assertTrue(result.isEmpty());
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesOneProperty() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, Arrays.asList("fOne"));

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "propertyone"));
    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesOnePropertyBrokenClassNameCase() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(
            database, "ClaSSforindeXmanagerTEST", Arrays.asList("fOne"));

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "propertyone"));
    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesTwoProperties() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, Arrays.asList("fTwo", "fOne"));
    assertEquals(result.size(), 2);

    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesThreeProperties() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(
            database, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "compositetwo");
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesThreePropertiesBrokenFiledNameTest() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(
            database, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "compositetwo");
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesNotInvolvedProperties() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, Arrays.asList("fTwo", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeeded() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(
            database, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetClassInvolvedIndexesWithNullValues() {
    String className = "GetClassInvolvedIndexesWithNullValues";
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.createClass(className);

    oClass.createProperty("one", OType.STRING);
    oClass.createProperty("two", OType.STRING);
    oClass.createProperty("three", OType.STRING);

    indexManager.createIndex(
        database,
        className + "_indexOne_notunique",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new OPropertyIndexDefinition(className, "one", OType.STRING),
        oClass.getClusterIds(),
        null,
        null);

    indexManager.createIndex(
        database,
        className + "_indexOneTwo_notunique",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new OCompositeIndexDefinition(
            className,
            Arrays.asList(
                new OPropertyIndexDefinition(className, "one", OType.STRING),
                new OPropertyIndexDefinition(className, "two", OType.STRING)),
            -1),
        oClass.getClusterIds(),
        null,
        null);

    indexManager.createIndex(
        database,
        className + "_indexOneTwoThree_notunique",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new OCompositeIndexDefinition(
            className,
            Arrays.asList(
                new OPropertyIndexDefinition(className, "one", OType.STRING),
                new OPropertyIndexDefinition(className, "two", OType.STRING),
                new OPropertyIndexDefinition(className, "three", OType.STRING)),
            -1),
        oClass.getClusterIds(),
        null,
        null);

    Set<OIndex> result =
        indexManager.getClassInvolvedIndexes(database, className, Arrays.asList("one"));
    assertEquals(result.size(), 3);

    result = indexManager.getClassInvolvedIndexes(database, className, Arrays.asList("one", "two"));
    assertEquals(result.size(), 2);

    result =
        indexManager.getClassInvolvedIndexes(
            database, className, Arrays.asList("one", "two", "three"));
    assertEquals(result.size(), 1);

    result = indexManager.getClassInvolvedIndexes(database, className, Arrays.asList("two"));
    assertEquals(result.size(), 0);

    result =
        indexManager.getClassInvolvedIndexes(
            database, className, Arrays.asList("two", "one", "three"));
    assertEquals(result.size(), 1);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexes() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> indexes = indexManager.getClassIndexes(database, CLASS_NAME);
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

    final OPropertyIndexDefinition propertyIndex =
        new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER);
    propertyIndex.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(propertyIndex);

    assertEquals(indexes.size(), 3);

    for (final OIndex index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexesBrokenClassNameCase() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<OIndex> indexes = indexManager.getClassIndexes(database, "ClassforindeXMaNAgerTeST");
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

    final OPropertyIndexDefinition propertyIndex =
        new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER);
    propertyIndex.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(propertyIndex);

    assertEquals(indexes.size(), 3);

    for (final OIndex index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }
  }

  @Test
  public void testDropIndex() throws Exception {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    indexManager.createIndex(
        database,
        "anotherproperty",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER),
        new int[] {database.getClusterIdByName(CLASS_NAME)},
        null,
        null);

    assertNotNull(indexManager.getIndex(database, "anotherproperty"));
    assertNotNull(indexManager.getClassIndex(database, CLASS_NAME, "anotherproperty"));

    indexManager.dropIndex(database, "anotherproperty");

    assertNull(indexManager.getIndex(database, "anotherproperty"));
    assertNull(indexManager.getClassIndex(database, CLASS_NAME, "anotherproperty"));
  }

  @Test
  public void testDropAllClassIndexes() {
    final OClass oClass =
        database.getMetadata().getSchema().createClass("indexManagerTestClassTwo");
    oClass.createProperty("fOne", OType.INTEGER);

    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    indexManager.createIndex(
        database,
        "twoclassproperty",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        new OPropertyIndexDefinition("indexManagerTestClassTwo", "fOne", OType.INTEGER),
        new int[] {database.getClusterIdByName("indexManagerTestClassTwo")},
        null,
        null);

    assertFalse(indexManager.getClassIndexes(database, "indexManagerTestClassTwo").isEmpty());

    indexManager.dropIndex(database, "twoclassproperty");

    assertTrue(indexManager.getClassIndexes(database, "indexManagerTestClassTwo").isEmpty());
  }

  @Test(dependsOnMethods = "testDropAllClassIndexes")
  public void testDropNonExistingClassIndex() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    indexManager.dropIndex(database, "twoclassproperty");
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndex() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final OIndex result = indexManager.getClassIndex(database, CLASS_NAME, "propertyone");
    assertNotNull(result);
    assertEquals(result.getName(), "propertyone");
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexBrokenClassNameCase() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final OIndex result =
        indexManager.getClassIndex(database, "ClaSSforindeXManagerTeST", "propertyone");
    assertNotNull(result);
    assertEquals(result.getName(), "propertyone");
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexWrongIndexName() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final OIndex result = indexManager.getClassIndex(database, CLASS_NAME, "propertyonetwo");
    assertNull(result);
  }

  @Test(
      dependsOnMethods = {
        "createCompositeIndexTestWithListener",
        "createCompositeIndexTestWithoutListener",
        "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexWrongClassName() {
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final OIndex result = indexManager.getClassIndex(database, "testClassTT", "propertyone");
    assertNull(result);
  }

  private boolean containsIndex(
      final Collection<? extends OIndex> classIndexes, final String indexName) {
    for (final OIndex index : classIndexes) {
      if (index.getName().equals(indexName)) return true;
    }
    return false;
  }
}
