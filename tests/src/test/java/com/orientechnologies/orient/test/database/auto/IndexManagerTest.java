package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.testng.annotations.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.*;

@Test
public class IndexManagerTest {
    private static final String CLASS_NAME = "classForIndexManagerTest";
    private ODatabaseDocument databaseDocument;

    private String url;

    @Parameters(value = "url")
    public IndexManagerTest(String iURL) {
        url = iURL;
    }

    @BeforeClass
    public void beforeClass() {
        databaseDocument = new ODatabaseDocumentTx(url);
        databaseDocument.open("admin", "admin");

        final OSchema schema = databaseDocument.getMetadata().getSchema();

        final OClass oClass = schema.createClass(CLASS_NAME);


        oClass.createProperty("fOne", OType.INTEGER);
        oClass.createProperty("fTwo", OType.STRING);
        oClass.createProperty("fThree", OType.BOOLEAN);
        oClass.createProperty("fFour", OType.INTEGER);

        oClass.createProperty("fSix", OType.STRING);
        oClass.createProperty("fSeven", OType.STRING);

        schema.save();
        databaseDocument.close();
    }

    @BeforeMethod
    public void beforeMethod() {
        databaseDocument.open("admin", "admin");
    }

    @AfterMethod
    public void afterMethod() {
        databaseDocument.close();
    }

    @AfterClass
    public void afterClass() {
        databaseDocument.open("admin", "admin");
        databaseDocument.getMetadata().getSchema().dropClass(CLASS_NAME);
        databaseDocument.getMetadata().getSchema().dropClass("indexManagerTestClassTwo");
        databaseDocument.close();
    }

    @Test
    public void testCreateSimpleKeyIndexTest() {
        final OIndexManagerProxy indexManager = databaseDocument.getMetadata().getIndexManager();

        final OIndex result = indexManager.createIndex("simplekey", OClass.INDEX_TYPE.UNIQUE.toString(),
                new OSimpleKeyIndexDefinition(OType.INTEGER),
                null, null);

        assertEquals(result.getName(), "simplekey");

        indexManager.reload();
        assertNull(databaseDocument.getMetadata().getIndexManager().getClassIndex(CLASS_NAME, "simplekey"));
        assertEquals(databaseDocument.getMetadata().getIndexManager().getIndex("simplekey").getName(), result.getName());
    }

    @Test
    public void testCreateNullKeyDefinitionIndexTest() {
        final OIndexManagerProxy indexManager = databaseDocument.getMetadata().getIndexManager();

        final OIndex result = indexManager.createIndex("nullkey", OClass.INDEX_TYPE.UNIQUE.toString(),
                null, null, null);

        assertEquals(result.getName(), "nullkey");
        indexManager.reload();

        assertNull(databaseDocument.getMetadata().getIndexManager().getClassIndex(CLASS_NAME, "nullkey"));
        assertEquals(databaseDocument.getMetadata().getIndexManager().getIndex("nullkey").getName(), result.getName());
    }


    @Test
    public void testCreateOnePropertyIndexTest() {
        final OIndexManagerProxy indexManager = databaseDocument.getMetadata().getIndexManager();

        final OIndex result = indexManager.createIndex("propertyone", OClass.INDEX_TYPE.UNIQUE.toString(),
                new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER),
                new int[]{databaseDocument.getClusterIdByName(CLASS_NAME)}, null);

        assertEquals(result.getName(), "propertyone");

        indexManager.reload();
        assertEquals(databaseDocument.getMetadata().getIndexManager().getClassIndex(CLASS_NAME, "propertyone").getName(),
                result.getName());

    }

    @Test
    public void createCompositeIndexTestWithoutListener() {
        final OIndexManagerProxy indexManager = databaseDocument.getMetadata().getIndexManager();

        final OIndex result = indexManager.createIndex("compositeone", OClass.INDEX_TYPE.NOTUNIQUE.toString(),
                new OCompositeIndexDefinition(CLASS_NAME, Arrays.asList(
                        new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER),
                        new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING)

                )), new int[]{databaseDocument.getClusterIdByName(CLASS_NAME)}, null);

        assertEquals(result.getName(), "compositeone");

        indexManager.reload();
        assertEquals(databaseDocument.getMetadata().getIndexManager().getClassIndex(CLASS_NAME, "compositeone").getName(),
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

        final OIndexManagerProxy indexManager = databaseDocument.getMetadata().getIndexManager();

        final OIndex result = indexManager.createIndex("compositetwo", OClass.INDEX_TYPE.NOTUNIQUE.toString(),
                new OCompositeIndexDefinition(CLASS_NAME, Arrays.asList(
                        new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER),
                        new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING),
                        new OPropertyIndexDefinition(CLASS_NAME, "fThree", OType.BOOLEAN)

                )), new int[]{databaseDocument.getClusterIdByName(CLASS_NAME)}, progressListener);

        assertEquals(result.getName(), "compositetwo");
        assertEquals(atomicInteger.get(), 2);

        indexManager.reload();
        assertEquals(databaseDocument.getMetadata().getIndexManager().getClassIndex(CLASS_NAME, "compositetwo").getName(),
                result.getName());

    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedOneProperty() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fOne"));

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedDoesNotContainProperty() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fSix"));

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedTwoProperties() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne"));

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedThreeProperties() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedThreePropertiesBrokenFiledNameCase() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("ftwO", "Fone", "fThrEE"));

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedThreePropertiesBrokenClassNameCase() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed("ClaSSForIndeXManagerTeST", Arrays.asList("fTwo", "fOne", "fThree"));

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedPropertiesNotFirst() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fTree"));

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedPropertiesMoreThanNeeded() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedOnePropertyArrayParams() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, "fOne");

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedDoesNotContainPropertyArrayParams() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, "fSix");

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedTwoPropertiesArrayParams() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne");

        assertTrue(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedThreePropertiesArrayParams() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne", "fThree");

        assertTrue(result);
    }


    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedPropertiesNotFirstArrayParams() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fTree");

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testAreIndexedPropertiesMoreThanNeededArrayParams() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne", "fThee", "fFour");

        assertFalse(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesOnePropertyArrayParams() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, "fOne");

        assertEquals(result.size(), 3);

        assertTrue(containsIndex(result, "propertyone"));
        assertTrue(containsIndex(result, "compositeone"));
        assertTrue(containsIndex(result, "compositetwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesTwoPropertiesArrayParams() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, "fTwo", "fOne");
        assertEquals(result.size(), 2);

        assertTrue(containsIndex(result, "compositeone"));
        assertTrue(containsIndex(result, "compositetwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesThreePropertiesArrayParams() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, "fTwo", "fOne", "fThree");

        assertEquals(result.size(), 1);
        assertEquals(result.iterator().next().getName(), "compositetwo");
    }


    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesNotInvolvedPropertiesArrayParams() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, "fTwo", "fFour");

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesPropertiesMorThanNeededArrayParams() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, "fTwo", "fOne", "fThee", "fFour");

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetInvolvedIndexesPropertiesMorThanNeeded() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME,
                Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesNotExistingClass() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes("testlass", Arrays.asList("fOne"));

        assertTrue(result.isEmpty());
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesOneProperty() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, Arrays.asList("fOne"));

        assertEquals(result.size(), 3);

        assertTrue(containsIndex(result, "propertyone"));
        assertTrue(containsIndex(result, "compositeone"));
        assertTrue(containsIndex(result, "compositetwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesOnePropertyBrokenClassNameCase() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes("ClaSSforindeXmanagerTEST", Arrays.asList("fOne"));

        assertEquals(result.size(), 3);

        assertTrue(containsIndex(result, "propertyone"));
        assertTrue(containsIndex(result, "compositeone"));
        assertTrue(containsIndex(result, "compositetwo"));
    }


    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesTwoProperties() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, Arrays.asList("fTwo", "fOne"));
        assertEquals(result.size(), 2);

        assertTrue(containsIndex(result, "compositeone"));
        assertTrue(containsIndex(result, "compositetwo"));
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesThreeProperties() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME,
                Arrays.asList("fTwo", "fOne", "fThree"));

        assertEquals(result.size(), 1);
        assertEquals(result.iterator().next().getName(), "compositetwo");
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesThreePropertiesBrokenFiledNameTest() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME,
                Arrays.asList("ftwO", "foNe", "fThrEE"));

        assertEquals(result.size(), 1);
        assertEquals(result.iterator().next().getName(), "compositetwo");
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesNotInvolvedProperties() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME, Arrays.asList("fTwo", "fFour"));

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassInvolvedIndexesPropertiesMorThanNeeded() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> result = indexManager.getClassInvolvedIndexes(CLASS_NAME,
                Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

        assertEquals(result.size(), 0);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassIndexes() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> indexes = indexManager.getClassIndexes(CLASS_NAME);
        final Set<OIndexDefinition> expectedIndexDefinitions = new HashSet<OIndexDefinition>();

        final OCompositeIndexDefinition compositeIndexOne = new OCompositeIndexDefinition(CLASS_NAME);

        compositeIndexOne.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER));
        compositeIndexOne.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING));
        expectedIndexDefinitions.add(compositeIndexOne);

        final OCompositeIndexDefinition compositeIndexTwo = new OCompositeIndexDefinition(CLASS_NAME);

        compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER));
        compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING));
        compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fThree", OType.BOOLEAN));
        expectedIndexDefinitions.add(compositeIndexTwo);

        final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER);
        expectedIndexDefinitions.add(propertyIndex);

        assertEquals(indexes.size(), 3);

        for (final OIndex index : indexes) {
            assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
        }

    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassIndexesBrokenClassNameCase() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final Set<OIndex<?>> indexes = indexManager.getClassIndexes("ClassforindeXMaNAgerTeST");
        final Set<OIndexDefinition> expectedIndexDefinitions = new HashSet<OIndexDefinition>();

        final OCompositeIndexDefinition compositeIndexOne = new OCompositeIndexDefinition(CLASS_NAME);

        compositeIndexOne.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER));
        compositeIndexOne.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING));
        expectedIndexDefinitions.add(compositeIndexOne);

        final OCompositeIndexDefinition compositeIndexTwo = new OCompositeIndexDefinition(CLASS_NAME);

        compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER));
        compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fTwo", OType.STRING));
        compositeIndexTwo.addIndex(new OPropertyIndexDefinition(CLASS_NAME, "fThree", OType.BOOLEAN));
        expectedIndexDefinitions.add(compositeIndexTwo);

        final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER);
        expectedIndexDefinitions.add(propertyIndex);

        assertEquals(indexes.size(), 3);

        for (final OIndex index : indexes) {
            assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
        }

    }

    @Test
    public void testDropIndex() throws Exception {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        indexManager.createIndex("anotherproperty", OClass.INDEX_TYPE.UNIQUE.toString(),
                new OPropertyIndexDefinition(CLASS_NAME, "fOne", OType.INTEGER),
                new int[]{databaseDocument.getClusterIdByName(CLASS_NAME)}, null);

        assertNotNull(indexManager.getIndex("anotherproperty"));
        assertNotNull(indexManager.getClassIndex(CLASS_NAME, "anotherproperty"));


        indexManager.dropIndex("anotherproperty");

        assertNull(indexManager.getIndex("anotherproperty"));
        assertNull(indexManager.getClassIndex(CLASS_NAME, "anotherproperty"));
    }

    @Test
    public void testDropSimpleKey() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();
        indexManager.createIndex("simplekeytwo", OClass.INDEX_TYPE.UNIQUE.toString(),
                new OSimpleKeyIndexDefinition(OType.INTEGER),
                null, null);


        assertNotNull(indexManager.getIndex("simplekeytwo"));

        indexManager.dropIndex("simplekeytwo");

        assertNull(indexManager.getIndex("simplekeytwo"));
    }

    @Test
    public void testDropNullKeyDefinition() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        indexManager.createIndex("nullkeytwo", OClass.INDEX_TYPE.UNIQUE.toString(),
                null, null, null);

        assertNotNull(indexManager.getIndex("nullkeytwo"));

        indexManager.dropIndex("nullkeytwo");

        assertNull(indexManager.getIndex("nullkeytwo"));
    }

    @Test
    public void testDropAllClassIndexes() {
        final OClass oClass = databaseDocument.getMetadata().getSchema().createClass("indexManagerTestClassTwo");
        oClass.createProperty("fOne", OType.INTEGER);

        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        indexManager.createIndex("twoclassproperty", OClass.INDEX_TYPE.UNIQUE.toString(),
                new OPropertyIndexDefinition("indexManagerTestClassTwo", "fOne", OType.INTEGER),
                new int[]{databaseDocument.getClusterIdByName("indexManagerTestClassTwo")}, null);

        assertFalse(indexManager.getClassIndexes("indexManagerTestClassTwo").isEmpty());

        indexManager.dropIndex("twoclassproperty");

        assertTrue(indexManager.getClassIndexes("indexManagerTestClassTwo").isEmpty());
    }

    @Test(dependsOnMethods = "testDropAllClassIndexes")
    public void testDropNonExistingClassIndex() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        indexManager.dropIndex("twoclassproperty");
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassIndex() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final OIndex<?> result = indexManager.getClassIndex(CLASS_NAME, "propertyone");
        assertNotNull(result);
        assertEquals(result.getName(), "propertyone");
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassIndexBrokenClassNameCase() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final OIndex<?> result = indexManager.getClassIndex("ClaSSforindeXManagerTeST", "propertyone");
        assertNotNull(result);
        assertEquals(result.getName(), "propertyone");
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassIndexWrongIndexName() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

        final OIndex<?> result = indexManager.getClassIndex(CLASS_NAME, "propertyonetwo");
        assertNull(result);
    }

    @Test(dependsOnMethods = {"createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
            "testCreateOnePropertyIndexTest"})
    public void testGetClassIndexWrongClassName() {
        final OIndexManager indexManager = databaseDocument.getMetadata().getIndexManager();

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
