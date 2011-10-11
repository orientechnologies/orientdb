package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.annotations.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.*;

@Test(groups = {"index"})
public class ClassIndexTest {
    private final ODatabaseDocumentTx database;
    private OClass oClass;
    private OClass oSuperClass;

    @Parameters(value = "url")
    public ClassIndexTest(final String iURL) {
        database =  new ODatabaseDocumentTx(iURL);
    }

    @BeforeClass
    public void beforeClass() {
        if(database.isClosed())
            database.open("admin", "admin");

        final OSchema schema = database.getMetadata().getSchema();

        oClass = schema.createClass("ClassIndexTestClass");
        oSuperClass = schema.createClass("ClassIndexTestSuperClass");


        oClass.createProperty("fOne", OType.INTEGER);
        oClass.createProperty("fTwo", OType.STRING);
        oClass.createProperty("fThree", OType.BOOLEAN);
        oClass.createProperty("fFour", OType.INTEGER);

        oClass.createProperty("fSix", OType.STRING);
        oClass.createProperty("fSeven", OType.STRING);

        oSuperClass.createProperty("fNine", OType.INTEGER);
        oClass.setSuperClass(oSuperClass);

        schema.save();
        database.close();
    }

    @BeforeMethod
    public void beforeMethod() {
        database.open("admin", "admin");
    }

    @AfterMethod
    public void afterMethod() {
        database.close();
    }

    @AfterClass
    public void afterClass() {
        if(database.isClosed())
            database.open("admin", "admin");

        database.command(new OCommandSQL("delete from ClassIndexTestClass")).execute();
        database.command(new OCommandSQL("delete from ClassIndexTestSuperClass")).execute();
        database.command(new OCommandSQL("delete from ClassIndexInTest")).execute();

        database.command(new OCommandSQL("drop class ClassIndexInTest")).execute();
        database.command(new OCommandSQL("drop class ClassIndexTestClass")).execute();

        database.getMetadata().getSchema().reload();

        database.command(new OCommandSQL("drop class ClassIndexTestSuperClass")).execute();

        database.getMetadata().getSchema().reload();
        database.getMetadata().getIndexManager().reload();

        database.close();
    }

    @Test
    public void testCreateOnePropertyIndexTest() {
        final OIndex result = oClass.createIndex("ClassIndexTestPropertyOne", OClass.INDEX_TYPE.UNIQUE, "fOne");

        assertEquals(result.getName(), "ClassIndexTestPropertyOne");
        assertEquals(oClass.getClassIndex("ClassIndexTestPropertyOne").getName(), result.getName());
        assertEquals(
                database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestPropertyOne").getName(),
                result.getName());

    }

    @Test
    public void createCompositeIndexTestWithoutListener() {
        final OIndex result = oClass.createIndex("ClassIndexTestCompositeOne", OClass.INDEX_TYPE.UNIQUE, "fOne", "fTwo");

        assertEquals(result.getName(), "ClassIndexTestCompositeOne");
        assertEquals(oClass.getClassIndex("ClassIndexTestCompositeOne").getName(), result.getName());
        assertEquals(database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeOne").getName(),
                result.getName());
    }

    @Test
    public void createCompositeIndexTestWithListener() {
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final OProgressListener progressListener = new OProgressListener() {
            public void onBegin(final Object iTask, final long iTotal) {
                atomicInteger.incrementAndGet();
            }

            public boolean onProgress(final Object iTask, final long iCounter, final float iPercent) {
                return true;
            }

            public void onCompletition(final Object iTask, final boolean iSucceed) {
                atomicInteger.incrementAndGet();
            }
        };

        final OIndex result = oClass.createIndex("ClassIndexTestCompositeTwo", OClass.INDEX_TYPE.UNIQUE,
                progressListener, "fOne", "fTwo", "fThree");

        assertEquals(result.getName(), "ClassIndexTestCompositeTwo");
        assertEquals(oClass.getClassIndex("ClassIndexTestCompositeTwo").getName(), result.getName());
        assertEquals(database.getMetadata().getIndexManager().getClassIndex("ClassIndexTestClass", "ClassIndexTestCompositeTwo").getName(),
                result.getName());
        assertEquals(atomicInteger.get(), 2);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedOneProperty() {
        final boolean result = oClass.areIndexed(Arrays.asList("fOne"));

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedDoesNotContainProperty() {
        final boolean result = oClass.areIndexed(Arrays.asList("fSix"));

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedTwoProperties() {
        final boolean result = oClass.areIndexed(Arrays.asList("fTwo", "fOne"));

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedThreeProperties() {
        final boolean result = oClass.areIndexed(Arrays.asList("fTwo", "fOne", "fThree"));

        assertTrue(result);
    }


    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedPropertiesNotFirst() {
        final boolean result = oClass.areIndexed(Arrays.asList("fTwo", "fTree"));

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedPropertiesMoreThanNeeded() {
        final boolean result = oClass.areIndexed(Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest", "createParentPropertyIndex"})
    public void testAreIndexedParentProperty() {
        final boolean result = oClass.areIndexed(Arrays.asList("fNine"));

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedParentChildProperty() {
        final boolean result = oClass.areIndexed(Arrays.asList("fOne, fNine"));

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedOnePropertyArrayParams() {
        final boolean result = oClass.areIndexed("fOne");

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedDoesNotContainPropertyArrayParams() {
        final boolean result = oClass.areIndexed("fSix");

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedTwoPropertiesArrayParams() {
        final boolean result = oClass.areIndexed("fTwo", "fOne");

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedThreePropertiesArrayParams() {
        final boolean result = oClass.areIndexed("fTwo", "fOne", "fThree");

        assertTrue(result);
    }


    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedPropertiesNotFirstArrayParams() {
        final boolean result = oClass.areIndexed("fTwo", "fTree");

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedPropertiesMoreThanNeededArrayParams() {
        final boolean result = oClass.areIndexed("fTwo", "fOne", "fThee", "fFour");

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest", "createParentPropertyIndex"})
    public void testAreIndexedParentPropertyArrayParams() {
        final boolean result = oClass.areIndexed("fNine");

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedParentChildPropertyArrayParams() {
        final boolean result = oClass.areIndexed("fOne, fNine");

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesOnePropertyArrayParams() {
        final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes("fOne");

        assertEquals(result.size(), 3);

        assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesTwoPropertiesArrayParams() {
        final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes("fTwo", "fOne");
        assertEquals(result.size(), 2);

        assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesThreePropertiesArrayParams() {
        final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes("fTwo", "fOne", "fThree");

        assertEquals(result.size(), 1);
        assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
    }


    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesNotInvolvedPropertiesArrayParams() {
        final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes("fTwo", "fFour");

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesPropertiesMorThanNeededArrayParams() {
        final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes("fTwo", "fOne", "fThee", "fFour");

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetInvolvedIndexesPropertiesMorThanNeeded() {
        final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesOneProperty() {
        final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fOne"));

        assertEquals(result.size(), 3);

        assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesTwoProperties() {
        final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fTwo", "fOne"));
        assertEquals(result.size(), 2);

        assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesThreeProperties() {
        final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fTwo", "fOne", "fThree"));

        assertEquals(result.size(), 1);
        assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesNotInvolvedProperties() {
        final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fTwo", "fFour"));

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesPropertiesMorThanNeeded() {
        final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes(Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetInvolvedIndexesOnePropertyArrayParams() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fOne");

        assertEquals(result.size(), 3);

        assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetInvolvedIndexesTwoPropertiesArrayParams() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fTwo", "fOne");
        assertEquals(result.size(), 2);

        assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetInvolvedIndexesThreePropertiesArrayParams() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fTwo", "fOne", "fThree");

        assertEquals(result.size(), 1);
        assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
    }


    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetInvolvedIndexesNotInvolvedPropertiesArrayParams() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fTwo", "fFour");

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetParentInvolvedIndexesArrayParams() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fNine");

        assertEquals(result.size(), 1);
        assertEquals(result.iterator().next().getName(), "ClassIndexTestParentPropertyNine");
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetParentChildInvolvedIndexesArrayParams() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes("fOne", "fNine");

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetInvolvedIndexesOneProperty() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fOne"));

        assertEquals(result.size(), 3);

        assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetInvolvedIndexesTwoProperties() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fTwo", "fOne"));
        assertEquals(result.size(), 2);

        assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
        assertTrue(containsIndex(result, "ClassIndexTestCompositeTwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetInvolvedIndexesThreeProperties() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fTwo", "fOne", "fThree"));

        assertEquals(result.size(), 1);
        assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
    }


    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetInvolvedIndexesNotInvolvedProperties() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fTwo", "fFour"));

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetParentInvolvedIndexes() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fNine"));

        assertEquals(result.size(), 1);
        assertEquals(result.iterator().next().getName(), "ClassIndexTestParentPropertyNine");
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetParentChildInvolvedIndexes() {
        final Set<OIndex<?>> result = oClass.getInvolvedIndexes(Arrays.asList("fOne", "fNine"));

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
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

        final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", OType.INTEGER);
        expectedIndexDefinitions.add(propertyIndex);

        assertEquals(indexes.size(), 3);

        for (final OIndex index : indexes) {
            assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
        }

    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest", "createParentPropertyIndex"})
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

        final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", OType.INTEGER);
        expectedIndexDefinitions.add(propertyIndex);

        final OPropertyIndexDefinition parentPropertyIndex = new OPropertyIndexDefinition("ClassIndexTestSuperClass", "fNine", OType.INTEGER);
        expectedIndexDefinitions.add(parentPropertyIndex);

        assertEquals(indexes.size(), 4);

        for (final OIndex index : indexes) {
            assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
        }
    }

    @Test
    public void testGetIndexesWithoutParent() {

        final OClass inClass = database.getMetadata().getSchema().createClass("ClassIndexInTest");
        inClass.createProperty("fOne", OType.INTEGER);

        final OIndex result = inClass.createIndex("ClassIndexTestPropertyOne", OClass.INDEX_TYPE.UNIQUE, "fOne");

        assertEquals(result.getName(), "ClassIndexTestPropertyOne");
        assertEquals(inClass.getClassIndex("ClassIndexTestPropertyOne").getName(), result.getName());

        final Set<OIndex<?>> indexes = inClass.getIndexes();
        final OPropertyIndexDefinition propertyIndexDefinition = new OPropertyIndexDefinition("ClassIndexInTest", "fOne", OType.INTEGER);

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

    @Test(expectedExceptions = OIndexException.class)
    public void testCreateProxyIndex() {
        oClass.createIndex("ClassIndexTestProxyIndex", OClass.INDEX_TYPE.PROXY, "fOne");
    }

    @Test(expectedExceptions = OIndexException.class)
    public void testCreateFullTextIndexTwoProperties() {
        oClass.createIndex("ClassIndexTestFulltextIndex", OClass.INDEX_TYPE.FULLTEXT, "fSix", "fSeven");
    }

    @Test
    public void testCreateFullTextIndexOneProperty() {
        final OIndex<?> result = oClass.createIndex("ClassIndexTestFulltextIndex", OClass.INDEX_TYPE.FULLTEXT, "fSix");

        assertEquals(result.getName(), "ClassIndexTestFulltextIndex");
        assertEquals(oClass.getClassIndex("ClassIndexTestFulltextIndex").getName(), result.getName());
        assertEquals(result.getType(), OClass.INDEX_TYPE.FULLTEXT.toString());
    }

    @Test
    public void testCreateDictionaryIndex() {
        final OIndex<?> result = oClass.createIndex("ClassIndexTestDictionaryIndex", OClass.INDEX_TYPE.DICTIONARY, "fOne");

        assertEquals(result.getName(), "ClassIndexTestDictionaryIndex");
        assertEquals(oClass.getClassIndex("ClassIndexTestDictionaryIndex").getName(), result.getName());
        assertEquals(result.getType(), OClass.INDEX_TYPE.DICTIONARY.toString());
    }

    @Test
    public void testCreateNotUniqueIndex() {
        final OIndex<?> result = oClass.createIndex("ClassIndexTestNotUniqueIndex", OClass.INDEX_TYPE.NOTUNIQUE, "fOne");

        assertEquals(result.getName(), "ClassIndexTestNotUniqueIndex");
        assertEquals(oClass.getClassIndex("ClassIndexTestNotUniqueIndex").getName(), result.getName());
        assertEquals(result.getType(), OClass.INDEX_TYPE.NOTUNIQUE.toString());
    }

    public void createParentPropertyIndex() {
        final OIndex result = oSuperClass.createIndex("ClassIndexTestParentPropertyNine", OClass.INDEX_TYPE.UNIQUE, "fNine");

        assertEquals(result.getName(), "ClassIndexTestParentPropertyNine");
        assertEquals(oSuperClass.getClassIndex("ClassIndexTestParentPropertyNine").getName(), result.getName());
    }

    private boolean containsIndex(final Collection<? extends OIndex> classIndexes, final String indexName) {
        for (final OIndex index : classIndexes) {
            if (index.getName().equals(indexName))
                return true;
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
