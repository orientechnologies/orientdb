package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.listener.OProgressListener;
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
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = { "index" })
public class ClassIndexTest extends DocumentDBBaseTest {

  private OClass oClass;
  private OClass oSuperClass;

  @Parameters(value = "url")
  public ClassIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OSchema schema = database.getMetadata().getSchema();

    oClass = schema.createClass("ClassIndexTestClass");
    oSuperClass = schema.createClass("ClassIndexTestSuperClass");

    oClass.createProperty("fOne", OType.INTEGER);
    oClass.createProperty("fTwo", OType.STRING);
    oClass.createProperty("fThree", OType.BOOLEAN);
    oClass.createProperty("fFour", OType.INTEGER);

    oClass.createProperty("fSix", OType.STRING);
    oClass.createProperty("fSeven", OType.STRING);

    oClass.createProperty("fEight", OType.INTEGER);
    oClass.createProperty("fTen", OType.INTEGER);
    oClass.createProperty("fEleven", OType.INTEGER);
    oClass.createProperty("fTwelve", OType.INTEGER);
    oClass.createProperty("fThirteen", OType.INTEGER);
    oClass.createProperty("fFourteen", OType.INTEGER);
    oClass.createProperty("fFifteen", OType.INTEGER);

    oClass.createProperty("fEmbeddedMap", OType.EMBEDDEDMAP, OType.INTEGER);
    oClass.createProperty("fEmbeddedMapWithoutLinkedType", OType.EMBEDDEDMAP);
    oClass.createProperty("fLinkMap", OType.LINKMAP);

    oClass.createProperty("fLinkList", OType.LINKLIST);
    oClass.createProperty("fEmbeddedList", OType.EMBEDDEDLIST, OType.INTEGER);

    oClass.createProperty("fEmbeddedSet", OType.EMBEDDEDSET, OType.INTEGER);
    oClass.createProperty("fLinkSet", OType.LINKSET);

    oClass.createProperty("fRidBag", OType.LINKBAG);

    oSuperClass.createProperty("fNine", OType.INTEGER);
    oClass.setSuperClass(oSuperClass);

    schema.save();
    database.close();
  }

  @Test
  public void testCreateOnePropertyIndexTest() {
    final OIndex result = oClass.createIndex("ClassIndexTestPropertyOne", OClass.INDEX_TYPE.UNIQUE, "fOne");

    assertEquals(result.getName(), "ClassIndexTestPropertyOne");
    assertEquals(oClass.getClassIndex("ClassIndexTestPropertyOne").getName(), result.getName());
    assertEquals(database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestPropertyOne")
        .getName(), result.getName());

  }

  @Test
  public void testCreateOnePropertyIndexInvalidName() {
    try {
      oClass.createIndex("ClassIndex:TestPropertyOne", OClass.INDEX_TYPE.UNIQUE, "fOne");
      fail();
    } catch (Exception e) {
      if (e instanceof OResponseProcessingException)
        e = (Exception) e.getCause();

      if (e.getCause() != null)
        e = (Exception) e.getCause();

      assertTrue(e instanceof IllegalArgumentException);
    }
  }

  @Test
  public void createCompositeIndexTestWithoutListener() {
    final OIndex result = oClass.createIndex("ClassIndexTestCompositeOne", OClass.INDEX_TYPE.UNIQUE, "fOne", "fTwo");

    assertEquals(result.getName(), "ClassIndexTestCompositeOne");
    assertEquals(oClass.getClassIndex("ClassIndexTestCompositeOne").getName(), result.getName());
    assertEquals(database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeOne")
        .getName(), result.getName());
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

    final OIndex result = oClass.createIndex("ClassIndexTestCompositeTwo", OClass.INDEX_TYPE.UNIQUE, progressListener, "fOne",
        "fTwo", "fThree");

    assertEquals(result.getName(), "ClassIndexTestCompositeTwo");
    assertEquals(oClass.getClassIndex("ClassIndexTestCompositeTwo").getName(), result.getName());
    assertEquals(database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeTwo")
        .getName(), result.getName());
    assertEquals(atomicInteger.get(), 2);
  }

  @Test
  public void testCreateOnePropertyEmbeddedMapIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestPropertyEmbeddedMap", OClass.INDEX_TYPE.UNIQUE, "fEmbeddedMap");

    assertEquals(result.getName(), "ClassIndexTestPropertyEmbeddedMap");
    assertEquals(oClass.getClassIndex("ClassIndexTestPropertyEmbeddedMap").getName(), result.getName());
    assertEquals(database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestPropertyEmbeddedMap")
        .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fEmbeddedMap");
    assertEquals(indexDefinition.getTypes()[0], OType.STRING);
    assertEquals(((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateCompositeEmbeddedMapIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestCompositeEmbeddedMap", OClass.INDEX_TYPE.UNIQUE, "fFifteen",
        "fEmbeddedMap");

    assertEquals(result.getName(), "ClassIndexTestCompositeEmbeddedMap");
    assertEquals(oClass.getClassIndex("ClassIndexTestCompositeEmbeddedMap").getName(), result.getName());
    assertEquals(database.getMetadata().getIndexManager()
        .getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeEmbeddedMap").getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[] { "fFifteen", "fEmbeddedMap" });

    assertEquals(indexDefinition.getTypes(), new OType[] { OType.INTEGER, OType.STRING });
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedMapByKeyIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestCompositeEmbeddedMapByKey", OClass.INDEX_TYPE.UNIQUE, "fEight",
        "fEmbeddedMap");

    assertEquals(result.getName(), "ClassIndexTestCompositeEmbeddedMapByKey");
    assertEquals(oClass.getClassIndex("ClassIndexTestCompositeEmbeddedMapByKey").getName(), result.getName());
    assertEquals(
        database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeEmbeddedMapByKey")
            .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[] { "fEight", "fEmbeddedMap" });

    assertEquals(indexDefinition.getTypes(), new OType[] { OType.INTEGER, OType.STRING });
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedMapByValueIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestCompositeEmbeddedMapByValue", OClass.INDEX_TYPE.UNIQUE, "fTen",
        "fEmbeddedMap by value");

    assertEquals(result.getName(), "ClassIndexTestCompositeEmbeddedMapByValue");
    assertEquals(oClass.getClassIndex("ClassIndexTestCompositeEmbeddedMapByValue").getName(), result.getName());
    assertEquals(
        database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeEmbeddedMapByValue")
            .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[] { "fTen", "fEmbeddedMap" });

    assertEquals(indexDefinition.getTypes(), new OType[] { OType.INTEGER, OType.INTEGER });
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeLinkMapByValueIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestCompositeLinkMapByValue", OClass.INDEX_TYPE.UNIQUE, "fEleven",
        "fLinkMap by value");

    assertEquals(result.getName(), "ClassIndexTestCompositeLinkMapByValue");
    assertEquals(oClass.getClassIndex("ClassIndexTestCompositeLinkMapByValue").getName(), result.getName());
    assertEquals(
        database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeLinkMapByValue")
            .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[] { "fEleven", "fLinkMap" });

    assertEquals(indexDefinition.getTypes(), new OType[] { OType.INTEGER, OType.LINK });
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedSetIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestCompositeEmbeddedSet", OClass.INDEX_TYPE.UNIQUE, "fTwelve",
        "fEmbeddedSet");

    assertEquals(result.getName(), "ClassIndexTestCompositeEmbeddedSet");
    assertEquals(oClass.getClassIndex("ClassIndexTestCompositeEmbeddedSet").getName(), result.getName());
    assertEquals(database.getMetadata().getIndexManager()
        .getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeEmbeddedSet").getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[] { "fTwelve", "fEmbeddedSet" });

    assertEquals(indexDefinition.getTypes(), new OType[] { OType.INTEGER, OType.INTEGER });
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test(dependsOnMethods = "testGetIndexes")
  public void testCreateCompositeLinkSetIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestCompositeLinkSet", OClass.INDEX_TYPE.UNIQUE, "fTwelve", "fLinkSet");

    assertEquals(result.getName(), "ClassIndexTestCompositeLinkSet");
    assertEquals(oClass.getClassIndex("ClassIndexTestCompositeLinkSet").getName(), result.getName());
    assertEquals(database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeLinkSet")
        .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[] { "fTwelve", "fLinkSet" });

    assertEquals(indexDefinition.getTypes(), new OType[] { OType.INTEGER, OType.LINK });
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedListIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestCompositeEmbeddedList", OClass.INDEX_TYPE.UNIQUE, "fThirteen",
        "fEmbeddedList");

    assertEquals(result.getName(), "ClassIndexTestCompositeEmbeddedList");
    assertEquals(oClass.getClassIndex("ClassIndexTestCompositeEmbeddedList").getName(), result.getName());
    assertEquals(
        database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeEmbeddedList")
            .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[] { "fThirteen", "fEmbeddedList" });

    assertEquals(indexDefinition.getTypes(), new OType[] { OType.INTEGER, OType.INTEGER });
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  public void testCreateCompositeLinkListIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestCompositeLinkList", OClass.INDEX_TYPE.UNIQUE, "fFourteen", "fLinkList");

    assertEquals(result.getName(), "ClassIndexTestCompositeLinkList");
    assertEquals(oClass.getClassIndex("ClassIndexTestCompositeLinkList").getName(), result.getName());
    assertEquals(database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeLinkList")
        .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[] { "fFourteen", "fLinkList" });

    assertEquals(indexDefinition.getTypes(), new OType[] { OType.INTEGER, OType.LINK });
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  public void testCreateCompositeRidBagIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestCompositeRidBag", OClass.INDEX_TYPE.UNIQUE, "fFourteen", "fRidBag");

    assertEquals(result.getName(), "ClassIndexTestCompositeRidBag");
    assertEquals(oClass.getClassIndex("ClassIndexTestCompositeRidBag").getName(), result.getName());
    assertEquals(database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeRidBag")
        .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[] { "fFourteen", "fRidBag" });

    assertEquals(indexDefinition.getTypes(), new OType[] { OType.INTEGER, OType.LINK });
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateOnePropertyLinkedMapIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestPropertyLinkedMap", OClass.INDEX_TYPE.UNIQUE, "fLinkMap");

    assertEquals(result.getName(), "ClassIndexTestPropertyLinkedMap");
    assertEquals(oClass.getClassIndex("ClassIndexTestPropertyLinkedMap").getName(), result.getName());
    assertEquals(database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestPropertyLinkedMap")
        .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fLinkMap");
    assertEquals(indexDefinition.getTypes()[0], OType.STRING);
    assertEquals(((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateOnePropertyLinkMapByKeyIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestPropertyLinkedMapByKey", OClass.INDEX_TYPE.UNIQUE, "fLinkMap by key");

    assertEquals(result.getName(), "ClassIndexTestPropertyLinkedMapByKey");
    assertEquals(oClass.getClassIndex("ClassIndexTestPropertyLinkedMapByKey").getName(), result.getName());
    assertEquals(
        database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestPropertyLinkedMapByKey")
            .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fLinkMap");
    assertEquals(indexDefinition.getTypes()[0], OType.STRING);
    assertEquals(((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateOnePropertyLinkMapByValueIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestPropertyLinkedMapByValue", OClass.INDEX_TYPE.UNIQUE,
        "fLinkMap by value");

    assertEquals(result.getName(), "ClassIndexTestPropertyLinkedMapByValue");
    assertEquals(oClass.getClassIndex("ClassIndexTestPropertyLinkedMapByValue").getName(), result.getName());
    assertEquals(
        database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestPropertyLinkedMapByValue")
            .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fLinkMap");
    assertEquals(indexDefinition.getTypes()[0], OType.LINK);
    assertEquals(((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.VALUE);
  }

  @Test
  public void testCreateOnePropertyByKeyEmbeddedMapIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestPropertyByKeyEmbeddedMap", OClass.INDEX_TYPE.UNIQUE,
        "fEmbeddedMap by key");

    assertEquals(result.getName(), "ClassIndexTestPropertyByKeyEmbeddedMap");
    assertEquals(oClass.getClassIndex("ClassIndexTestPropertyByKeyEmbeddedMap").getName(), result.getName());
    assertEquals(
        database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestPropertyByKeyEmbeddedMap")
            .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fEmbeddedMap");
    assertEquals(indexDefinition.getTypes()[0], OType.STRING);
    assertEquals(((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateOnePropertyByValueEmbeddedMapIndex() {
    final OIndex result = oClass.createIndex("ClassIndexTestPropertyByValueEmbeddedMap", OClass.INDEX_TYPE.UNIQUE,
        "fEmbeddedMap by value");

    assertEquals(result.getName(), "ClassIndexTestPropertyByValueEmbeddedMap");
    assertEquals(oClass.getClassIndex("ClassIndexTestPropertyByValueEmbeddedMap").getName(), result.getName());
    assertEquals(
        database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestPropertyByValueEmbeddedMap")
            .getName(), result.getName());

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fEmbeddedMap");
    assertEquals(indexDefinition.getTypes()[0], OType.INTEGER);
    assertEquals(((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.VALUE);
  }

  @Test
  public void testCreateOnePropertyWrongSpecifierEmbeddedMapIndexOne() {
    boolean exceptionIsThrown = false;
    try {
      oClass.createIndex("ClassIndexTestPropertyWrongSpecifierEmbeddedMap", OClass.INDEX_TYPE.UNIQUE, "fEmbeddedMap by ttt");
    } catch (Exception e) {
      if (e instanceof OResponseProcessingException)
        e = (Exception) ((OResponseProcessingException) e).getCause();

      Assert.assertTrue(e instanceof IllegalArgumentException);
      exceptionIsThrown = true;
      assertEquals(e.getMessage(), "Illegal field name format, should be '<property> [by key|value]' but was 'fEmbeddedMap by ttt'");
    }

    assertTrue(exceptionIsThrown);
    assertNull(oClass.getClassIndex("ClassIndexTestPropertyWrongSpecifierEmbeddedMap"));
  }

  @Test
  public void testCreateOnePropertyWrongSpecifierEmbeddedMapIndexTwo() {
    boolean exceptionIsThrown = false;
    try {
      oClass.createIndex("ClassIndexTestPropertyWrongSpecifierEmbeddedMap", OClass.INDEX_TYPE.UNIQUE, "fEmbeddedMap b value");
    } catch (IllegalArgumentException e) {
      exceptionIsThrown = true;
      assertEquals(e.getMessage(),
          "Illegal field name format, should be '<property> [by key|value]' but was 'fEmbeddedMap b value'");
    }

    assertTrue(exceptionIsThrown);
    assertNull(oClass.getClassIndex("ClassIndexTestPropertyWrongSpecifierEmbeddedMap"));
  }

  @Test
  public void testCreateOnePropertyWrongSpecifierEmbeddedMapIndexThree() {
    boolean exceptionIsThrown = false;
    try {
      oClass.createIndex("ClassIndexTestPropertyWrongSpecifierEmbeddedMap", OClass.INDEX_TYPE.UNIQUE, "fEmbeddedMap by value t");
    } catch (IllegalArgumentException e) {
      exceptionIsThrown = true;
      assertEquals(e.getMessage(),
          "Illegal field name format, should be '<property> [by key|value]' but was 'fEmbeddedMap by value t'");
    }

    assertTrue(exceptionIsThrown);
    assertNull(oClass.getClassIndex("ClassIndexTestPropertyWrongSpecifierEmbeddedMap"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedOneProperty() {
    final boolean result = oClass.areIndexed(Arrays.asList("fOne"));

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapIndex", "testCreateCompositeEmbeddedMapByKeyIndex",
      "testCreateCompositeEmbeddedMapByValueIndex", "testCreateCompositeLinkMapByValueIndex",
      "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedEightProperty() {
    final boolean result = oClass.areIndexed(Arrays.asList("fEight"));
    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByKeyIndex",
      "testCreateCompositeEmbeddedMapByValueIndex", "testCreateCompositeLinkMapByValueIndex",
      "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedEightPropertyEmbeddedMap() {
    final boolean result = oClass.areIndexed(Arrays.asList("fEmbeddedMap", "fEight"));
    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedDoesNotContainProperty() {
    final boolean result = oClass.areIndexed(Arrays.asList("fSix"));

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedTwoProperties() {
    final boolean result = oClass.areIndexed(Arrays.asList("fTwo", "fOne"));

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedThreeProperties() {
    final boolean result = oClass.areIndexed(Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedPropertiesNotFirst() {
    final boolean result = oClass.areIndexed(Arrays.asList("fTwo", "fTree"));

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedPropertiesMoreThanNeeded() {
    final boolean result = oClass.areIndexed(Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "createParentPropertyIndex", "testCreateOnePropertyEmbeddedMapIndex",
      "testCreateOnePropertyByKeyEmbeddedMapIndex", "testCreateOnePropertyByValueEmbeddedMapIndex",
      "testCreateOnePropertyLinkedMapIndex", "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex",
      "testCreateCompositeEmbeddedMapIndex", "testCreateCompositeEmbeddedMapByKeyIndex",
      "testCreateCompositeEmbeddedMapByValueIndex", "testCreateCompositeLinkMapByValueIndex",
      "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedParentProperty() {
    final boolean result = oClass.areIndexed(Arrays.asList("fNine"));

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedParentChildProperty() {
    final boolean result = oClass.areIndexed(Arrays.asList("fOne, fNine"));

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedOnePropertyArrayParams() {
    final boolean result = oClass.areIndexed("fOne");

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedDoesNotContainPropertyArrayParams() {
    final boolean result = oClass.areIndexed("fSix");

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedTwoPropertiesArrayParams() {
    final boolean result = oClass.areIndexed("fTwo", "fOne");

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedThreePropertiesArrayParams() {
    final boolean result = oClass.areIndexed("fTwo", "fOne", "fThree");

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedPropertiesNotFirstArrayParams() {
    final boolean result = oClass.areIndexed("fTwo", "fTree");

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedPropertiesMoreThanNeededArrayParams() {
    final boolean result = oClass.areIndexed("fTwo", "fOne", "fThee", "fFour");

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "createParentPropertyIndex", "testCreateOnePropertyEmbeddedMapIndex",
      "testCreateOnePropertyByKeyEmbeddedMapIndex", "testCreateOnePropertyByValueEmbeddedMapIndex",
      "testCreateOnePropertyLinkedMapIndex", "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex",
      "testCreateCompositeEmbeddedMapIndex", "testCreateCompositeEmbeddedMapByKeyIndex",
      "testCreateCompositeEmbeddedMapByValueIndex", "testCreateCompositeLinkMapByValueIndex",
      "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedParentPropertyArrayParams() {
    final boolean result = oClass.areIndexed("fNine");

    assertTrue(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testAreIndexedParentChildPropertyArrayParams() {
    final boolean result = oClass.areIndexed("fOne, fNine");

    assertFalse(result);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetClassInvolvedIndexesOnePropertyArrayParams() {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes("fOne");

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetClassInvolvedIndexesTwoPropertiesArrayParams() {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes("fTwo", "fOne");
    assertEquals(result.size(), 2);

    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetClassInvolvedIndexesThreePropertiesArrayParams() {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes("fTwo", "fOne", "fThree");

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetClassInvolvedIndexesNotInvolvedPropertiesArrayParams() {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes("fTwo", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeededArrayParams() {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes("fTwo", "fOne", "fThee", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetInvolvedIndexesPropertiesMorThanNeeded() {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetClassInvolvedIndexesOneProperty() {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fOne"));

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetClassInvolvedIndexesTwoProperties() {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fTwo", "fOne"));
    assertEquals(result.size(), 2);

    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetClassInvolvedIndexesThreeProperties() {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fTwo", "fOne", "fThree"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetClassInvolvedIndexesNotInvolvedProperties() {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fTwo", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeeded() {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetInvolvedIndexesOnePropertyArrayParams() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fOne");

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetInvolvedIndexesTwoPropertiesArrayParams() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fTwo", "fOne");
    assertEquals(result.size(), 2);

    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetInvolvedIndexesThreePropertiesArrayParams() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fTwo", "fOne", "fThree");

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetInvolvedIndexesNotInvolvedPropertiesArrayParams() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fTwo", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetParentInvolvedIndexesArrayParams() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fNine");

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestParentPropertyNine");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetParentChildInvolvedIndexesArrayParams() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fOne", "fNine");

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetInvolvedIndexesOneProperty() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fOne"));

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetInvolvedIndexesTwoProperties() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fTwo", "fOne"));
    assertEquals(result.size(), 2);

    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
    assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetInvolvedIndexesThreeProperties() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fTwo", "fOne", "fThree"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetInvolvedIndexesNotInvolvedProperties() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fTwo", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetParentInvolvedIndexes() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fNine"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestParentPropertyNine");
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex" })
  public void testGetParentChildInvolvedIndexes() {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fOne", "fNine"));

    assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
      "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
      "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex", "testCreateCompositeEmbeddedMapIndex",
      "testCreateCompositeEmbeddedMapByKeyIndex", "testCreateCompositeEmbeddedMapByValueIndex",
      "testCreateCompositeLinkMapByValueIndex", "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex",
      "testCreateCompositeLinkListIndex", "testCreateCompositeRidBagIndex" })
  public void testGetClassIndexes() {
    final Set<OIndex<?>> indexes = oClass.getClassIndexes();
    final Set<OIndexDefinition> expectedIndexDefinitions = new HashSet<OIndexDefinition>();

    final OCompositeIndexDefinition compositeIndexOne = new OCompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexOne.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", OType.INTEGER));
    compositeIndexOne.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fTwo", OType.STRING));
    expectedIndexDefinitions.add(compositeIndexOne);

    final OCompositeIndexDefinition compositeIndexTwo = new OCompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexTwo.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", OType.INTEGER));
    compositeIndexTwo.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fTwo", OType.STRING));
    compositeIndexTwo.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fThree", OType.BOOLEAN));
    expectedIndexDefinitions.add(compositeIndexTwo);

    final OCompositeIndexDefinition compositeIndexThree = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexThree.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fEight", OType.INTEGER));
    compositeIndexThree.addIndex(new OPropertyMapIndexDefinition("ClassIndexTestClass", "fEmbeddedMap", OType.STRING,
        OPropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexThree);

    final OCompositeIndexDefinition compositeIndexFour = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFour.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fTen", OType.INTEGER));
    compositeIndexFour.addIndex(new OPropertyMapIndexDefinition("ClassIndexTestClass", "fEmbeddedMap", OType.INTEGER,
        OPropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFour);

    final OCompositeIndexDefinition compositeIndexFive = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFive.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fEleven", OType.INTEGER));
    compositeIndexFive.addIndex(new OPropertyMapIndexDefinition("ClassIndexTestClass", "fLinkMap", OType.LINK,
        OPropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFive);

    final OCompositeIndexDefinition compositeIndexSix = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSix.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fTwelve", OType.INTEGER));
    compositeIndexSix.addIndex(new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedSet", OType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSix);

    final OCompositeIndexDefinition compositeIndexSeven = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSeven.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fThirteen", OType.INTEGER));
    compositeIndexSeven.addIndex(new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList", OType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSeven);

    final OCompositeIndexDefinition compositeIndexEight = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEight.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", OType.INTEGER));
    compositeIndexEight.addIndex(new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList", OType.LINK));
    expectedIndexDefinitions.add(compositeIndexEight);

    final OCompositeIndexDefinition compositeIndexNine = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexNine.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fFifteen", OType.INTEGER));
    compositeIndexNine.addIndex(new OPropertyMapIndexDefinition("ClassIndexTestClass", "fEmbeddedMap", OType.STRING,
        OPropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexNine);

    final OCompositeIndexDefinition compositeIndexTen = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexTen.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", OType.INTEGER));
    compositeIndexTen.addIndex(new OPropertyListIndexDefinition("ClassIndexTestClass", "fLinkList", OType.LINK));
    expectedIndexDefinitions.add(compositeIndexTen);

    final OCompositeIndexDefinition compositeIndexEleven = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEleven.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", OType.INTEGER));
    compositeIndexEleven.addIndex(new OPropertyRidBagIndexDefinition("ClassIndexTestClass", "fRidBag"));
    expectedIndexDefinitions.add(compositeIndexEleven);

    final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", OType.INTEGER);
    expectedIndexDefinitions.add(propertyIndex);

    final OPropertyMapIndexDefinition propertyMapIndexDefinition = new OPropertyMapIndexDefinition("ClassIndexTestClass",
        "fEmbeddedMap", OType.STRING, OPropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyMapIndexDefinition);

    final OPropertyMapIndexDefinition propertyMapByValueIndexDefinition = new OPropertyMapIndexDefinition("ClassIndexTestClass",
        "fEmbeddedMap", OType.INTEGER, OPropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyMapByValueIndexDefinition);

    final OPropertyMapIndexDefinition propertyLinkMapByKeyIndexDefinition = new OPropertyMapIndexDefinition("ClassIndexTestClass",
        "fLinkMap", OType.STRING, OPropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyLinkMapByKeyIndexDefinition);

    final OPropertyMapIndexDefinition propertyLinkMapByValueIndexDefinition = new OPropertyMapIndexDefinition(
        "ClassIndexTestClass", "fLinkMap", OType.LINK, OPropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyLinkMapByValueIndexDefinition);

    assertEquals(indexes.size(), 17);

    for (final OIndex index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }

  }

  @Test(dependsOnMethods = { "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
      "testCreateOnePropertyIndexTest", "createParentPropertyIndex", "testCreateOnePropertyEmbeddedMapIndex",
      "testCreateOnePropertyByKeyEmbeddedMapIndex", "testCreateOnePropertyByValueEmbeddedMapIndex",
      "testCreateOnePropertyLinkedMapIndex", "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex",
      "testCreateCompositeEmbeddedMapIndex", "testCreateCompositeEmbeddedMapByKeyIndex",
      "testCreateCompositeEmbeddedMapByValueIndex", "testCreateCompositeLinkMapByValueIndex",
      "testCreateCompositeEmbeddedSetIndex", "testCreateCompositeEmbeddedListIndex", "testCreateCompositeLinkListIndex",
      "testCreateCompositeRidBagIndex" })
  public void testGetIndexes() {
    final Set<OIndex<?>> indexes = oClass.getIndexes();
    final Set<OIndexDefinition> expectedIndexDefinitions = new HashSet<OIndexDefinition>();

    final OCompositeIndexDefinition compositeIndexOne = new OCompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexOne.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", OType.INTEGER));
    compositeIndexOne.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fTwo", OType.STRING));
    expectedIndexDefinitions.add(compositeIndexOne);

    final OCompositeIndexDefinition compositeIndexTwo = new OCompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexTwo.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", OType.INTEGER));
    compositeIndexTwo.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fTwo", OType.STRING));
    compositeIndexTwo.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fThree", OType.BOOLEAN));
    expectedIndexDefinitions.add(compositeIndexTwo);

    final OCompositeIndexDefinition compositeIndexThree = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexThree.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fEight", OType.INTEGER));
    compositeIndexThree.addIndex(new OPropertyMapIndexDefinition("ClassIndexTestClass", "fEmbeddedMap", OType.STRING,
        OPropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexThree);

    final OCompositeIndexDefinition compositeIndexFour = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFour.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fTen", OType.INTEGER));
    compositeIndexFour.addIndex(new OPropertyMapIndexDefinition("ClassIndexTestClass", "fEmbeddedMap", OType.INTEGER,
        OPropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFour);

    final OCompositeIndexDefinition compositeIndexFive = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFive.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fEleven", OType.INTEGER));
    compositeIndexFive.addIndex(new OPropertyMapIndexDefinition("ClassIndexTestClass", "fLinkMap", OType.LINK,
        OPropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFive);

    final OCompositeIndexDefinition compositeIndexSix = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSix.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fTwelve", OType.INTEGER));
    compositeIndexSix.addIndex(new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedSet", OType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSix);

    final OCompositeIndexDefinition compositeIndexSeven = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSeven.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fThirteen", OType.INTEGER));
    compositeIndexSeven.addIndex(new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList", OType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSeven);

    final OCompositeIndexDefinition compositeIndexEight = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEight.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", OType.INTEGER));
    compositeIndexEight.addIndex(new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList", OType.LINK));
    expectedIndexDefinitions.add(compositeIndexEight);

    final OCompositeIndexDefinition compositeIndexNine = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexNine.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fFifteen", OType.INTEGER));
    compositeIndexNine.addIndex(new OPropertyMapIndexDefinition("ClassIndexTestClass", "fEmbeddedMap", OType.STRING,
        OPropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexNine);

    final OCompositeIndexDefinition compositeIndexTen = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexTen.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", OType.INTEGER));
    compositeIndexTen.addIndex(new OPropertyListIndexDefinition("ClassIndexTestClass", "fLinkList", OType.LINK));
    expectedIndexDefinitions.add(compositeIndexTen);

    final OCompositeIndexDefinition compositeIndexEleven = new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEleven.addIndex(new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", OType.INTEGER));
    compositeIndexEleven.addIndex(new OPropertyRidBagIndexDefinition("ClassIndexTestClass", "fRidBag"));
    expectedIndexDefinitions.add(compositeIndexEleven);

    final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", OType.INTEGER);
    expectedIndexDefinitions.add(propertyIndex);

    final OPropertyIndexDefinition parentPropertyIndex = new OPropertyIndexDefinition("ClassIndexTestSuperClass", "fNine",
        OType.INTEGER);
    expectedIndexDefinitions.add(parentPropertyIndex);

    final OPropertyMapIndexDefinition propertyMapIndexDefinition = new OPropertyMapIndexDefinition("ClassIndexTestClass",
        "fEmbeddedMap", OType.STRING, OPropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyMapIndexDefinition);

    final OPropertyMapIndexDefinition propertyMapByValueIndexDefinition = new OPropertyMapIndexDefinition("ClassIndexTestClass",
        "fEmbeddedMap", OType.INTEGER, OPropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyMapByValueIndexDefinition);

    final OPropertyMapIndexDefinition propertyLinkMapByKeyIndexDefinition = new OPropertyMapIndexDefinition("ClassIndexTestClass",
        "fLinkMap", OType.STRING, OPropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyLinkMapByKeyIndexDefinition);

    final OPropertyMapIndexDefinition propertyLinkMapByValueIndexDefinition = new OPropertyMapIndexDefinition(
        "ClassIndexTestClass", "fLinkMap", OType.LINK, OPropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyLinkMapByValueIndexDefinition);

    assertEquals(indexes.size(), 18);

    for (final OIndex index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }
  }

  @Test
  public void testGetIndexesWithoutParent() {
    final OClass inClass = database.getMetadata().getSchema().createClass("ClassIndexInTest");
    inClass.createProperty("fOne", OType.INTEGER);

    final OIndex result = inClass.createIndex("ClassIndexInTestPropertyOne", OClass.INDEX_TYPE.UNIQUE, "fOne");

    assertEquals(result.getName(), "ClassIndexInTestPropertyOne");
    assertEquals(inClass.getClassIndex("ClassIndexInTestPropertyOne").getName(), result.getName());

    final Set<OIndex<?>> indexes = inClass.getIndexes();
    final OPropertyIndexDefinition propertyIndexDefinition = new OPropertyIndexDefinition("ClassIndexInTest", "fOne",
        OType.INTEGER);

    assertEquals(indexes.size(), 1);

    assertTrue(indexes.iterator().next().getDefinition().equals(propertyIndexDefinition));
  }

  @Test(expectedExceptions = OIndexException.class)
  public void testCreateIndexEmptyFields() {
    oClass.createIndex("ClassIndexTestCompositeEmpty", OClass.INDEX_TYPE.UNIQUE);
  }

  @Test(expectedExceptions = OIndexException.class)
  public void testCreateIndexAbsentFields() {
    oClass.createIndex("ClassIndexTestCompositeFieldAbsent", OClass.INDEX_TYPE.UNIQUE, "fFive");
  }

  @Test
  public void testCreateProxyIndex() {
    try {
      oClass.createIndex("ClassIndexTestProxyIndex", OClass.INDEX_TYPE.PROXY, "fOne");
      Assert.fail();
    } catch (OResponseProcessingException e) {
      Assert.assertTrue(e.getCause() instanceof OIndexException);
    } catch (OIndexException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(dependsOnMethods = "testAreIndexedDoesNotContainProperty")
  public void testCreateFullTextIndexTwoProperties() {
    try {
      oClass.createIndex("ClassIndexTestFulltextIndex", OClass.INDEX_TYPE.FULLTEXT, "fSix", "fSeven");
      Assert.fail();
    } catch (OResponseProcessingException e) {
      Assert.assertTrue(e.getCause() instanceof OIndexException);
    } catch (OIndexException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(dependsOnMethods = "testAreIndexedDoesNotContainProperty")
  public void testCreateFullTextIndexOneProperty() {
    final OIndex<?> result = oClass.createIndex("ClassIndexTestFulltextIndex", OClass.INDEX_TYPE.FULLTEXT, "fSix");

    assertEquals(result.getName(), "ClassIndexTestFulltextIndex");
    assertEquals(oClass.getClassIndex("ClassIndexTestFulltextIndex").getName(), result.getName());
    assertEquals(result.getType(), OClass.INDEX_TYPE.FULLTEXT.toString());
  }

  @Test(dependsOnMethods = "testGetInvolvedIndexesOnePropertyArrayParams")
  public void testCreateDictionaryIndex() {
    final OIndex<?> result = oClass.createIndex("ClassIndexTestDictionaryIndex", OClass.INDEX_TYPE.DICTIONARY, "fOne");

    assertEquals(result.getName(), "ClassIndexTestDictionaryIndex");
    assertEquals(oClass.getClassIndex("ClassIndexTestDictionaryIndex").getName(), result.getName());
    assertEquals(result.getType(), OClass.INDEX_TYPE.DICTIONARY.toString());
  }

  @Test(dependsOnMethods = "testGetInvolvedIndexesOnePropertyArrayParams")
  public void testCreateNotUniqueIndex() {
    final OIndex<?> result = oClass.createIndex("ClassIndexTestNotUniqueIndex", OClass.INDEX_TYPE.NOTUNIQUE, "fOne");

    assertEquals(result.getName(), "ClassIndexTestNotUniqueIndex");
    assertEquals(oClass.getClassIndex("ClassIndexTestNotUniqueIndex").getName(), result.getName());
    assertEquals(result.getType(), OClass.INDEX_TYPE.NOTUNIQUE.toString());
  }

  @Test
  public void testCreateMapWithoutLinkedType() {
    try {
      oClass.createIndex("ClassIndexMapWithoutLinkedTypeIndex", OClass.INDEX_TYPE.NOTUNIQUE,
          "fEmbeddedMapWithoutLinkedType by value");
      fail();
    } catch (OIndexException e) {
      assertEquals(e.getMessage(), "Linked type was not provided. "
          + "You should provide linked type for embedded collections that are going to be indexed.");
    }
  }

  public void createParentPropertyIndex() {
    final OIndex result = oSuperClass.createIndex("ClassIndexTestParentPropertyNine", OClass.INDEX_TYPE.UNIQUE, "fNine");

    assertEquals(result.getName(), "ClassIndexTestParentPropertyNine");
    assertEquals(oSuperClass.getClassIndex("ClassIndexTestParentPropertyNine").getName(), result.getName());
  }

  private boolean containsIndex(final Collection<? extends OIndex> classIndexes, final String indexName) {
    for (final OIndex index : classIndexes) {
      if (index.getName().equals(indexName)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testDropProperty() throws Exception {
    oClass.createProperty("fFive", OType.INTEGER);

    oClass.dropProperty("fFive");

    assertNull(oClass.getProperty("fFive"));
  }
}
