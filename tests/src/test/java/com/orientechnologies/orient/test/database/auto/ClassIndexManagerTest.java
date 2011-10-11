package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


@Test(groups = {"index"})
public class ClassIndexManagerTest {
    private final ODatabaseDocumentTx database;

    @Parameters(value = "url")
    public ClassIndexManagerTest(final String iURL) {
        database =  new ODatabaseDocumentTx(iURL);
    }


    @BeforeClass
    public void beforeClass() {
        if(database.isClosed())
            database.open("admin", "admin");

        final OSchema schema = database.getMetadata().getSchema();
        final OClass superClass = schema.createClass("classIndexManagerTestSuperClass");
        final OProperty propertyZero = superClass.createProperty("prop0", OType.STRING);
        propertyZero.createIndex(OClass.INDEX_TYPE.UNIQUE);

        final OClass oClass = schema.createClass("classIndexManagerTestClass", superClass);
        final OProperty propOne = oClass.createProperty("prop1", OType.STRING);
        propOne.createIndex(OClass.INDEX_TYPE.UNIQUE);

        final OProperty propTwo = oClass.createProperty("prop2", OType.INTEGER);
        propTwo.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

        oClass.createProperty("prop3", OType.BOOLEAN);

        oClass.createIndex("classIndexManagerComposite", OClass.INDEX_TYPE.UNIQUE, "prop1", "prop2");

        final OClass oClassTwo = schema.createClass("classIndexManagerTestClassTwo");
        oClassTwo.createProperty("prop1", OType.STRING);
        oClassTwo.createProperty("prop2", OType.INTEGER);

        schema.save();

        database.close();
    }

    @BeforeMethod
    public void beforeMethod() {
        if(database.isClosed())
            database.open("admin", "admin");
    }

    @AfterMethod
    public void afterMethod() {
        database.command(new OCommandSQL("delete from classIndexManagerTestClass")).execute();
        database.command(new OCommandSQL("delete from classIndexManagerTestClassTwo")).execute();
        database.command(new OCommandSQL("delete from classIndexManagerTestSuperClass")).execute();
        database.close();
    }

    @AfterClass
    public void afterClass() {
        if(database.isClosed())
            database.open("admin", "admin");
        database.command(new OCommandSQL("drop class classIndexManagerTestClass")).execute();
        database.command(new OCommandSQL("drop class classIndexManagerTestClassTwo")).execute();
        database.command(new OCommandSQL("drop class classIndexManagerTestSuperClass")).execute();
        database.close();
    }

    @Test
    public void testPropertiesCheckUniqueIndexDubKeysCreate() {
        final ODocument docOne = new ODocument(database, "classIndexManagerTestClass");
        final ODocument docTwo = new ODocument(database, "classIndexManagerTestClass");

        docOne.field("prop1", "a");
        docOne.save();

        boolean exceptionThrown = false;
        try {
            docTwo.field("prop1", "a");
            docTwo.save();
        } catch (OIndexException e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);
    }

    @Test
    public void testPropertiesCheckUniqueIndexInParentDubKeysCreate() {
        final ODocument docOne = new ODocument(database, "classIndexManagerTestClass");
        final ODocument docTwo = new ODocument(database, "classIndexManagerTestClass");

        docOne.field("prop0", "a");
        docOne.save();

        boolean exceptionThrown = false;
        try {
            docTwo.field("prop0", "a");
            docTwo.save();
        } catch (OIndexException e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);
    }


    @Test
    public void testPropertiesCheckUniqueIndexDubKeysUpdate() {
        final ODocument docOne = new ODocument(database, "classIndexManagerTestClass");
        final ODocument docTwo = new ODocument(database, "classIndexManagerTestClass");

        boolean exceptionThrown = false;
        docOne.field("prop1", "a");
        docOne.save();

        docTwo.field("prop1", "b");
        docTwo.save();


        try {
            docTwo.field("prop1", "a");
            docTwo.save();
        } catch (OIndexException e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);
    }


    @Test
    public void testPropertiesCheckNonUniqueIndexDubKeys() {
        final ODocument docOne = new ODocument(database, "classIndexManagerTestClass");
        docOne.field("prop2", 1);
        docOne.save();

        final ODocument docTwo = new ODocument(database, "classIndexManagerTestClass");
        docTwo.field("prop2", 1);
        docTwo.save();
    }

    @Test
    public void testPropertiesCheckUniqueNullKeys() {
        final ODocument docOne = new ODocument(database, "classIndexManagerTestClass");
        docOne.field("prop3", "a");
        docOne.save();

        final ODocument docTwo = new ODocument(database, "classIndexManagerTestClass");
        docTwo.field("prop3", "a");
        docTwo.save();
    }

    @Test
    public void testCreateDocumentWithoutClass() {
        final Collection<? extends OIndex> beforeIndexes = database.getMetadata().getIndexManager().getIndexes();
        final Map<String, Long> indexSizeMap = new HashMap<String, Long>();

        for(final OIndex index : beforeIndexes)
            indexSizeMap.put(index.getName(), index.getSize());

        final ODocument docOne = new ODocument(database);
        docOne.field("prop1", "a");
        docOne.save();

        final ODocument docTwo = new ODocument(database);
        docTwo.field("prop1", "a");
        docTwo.save();


        final Collection<? extends OIndex> afterIndexes = database.getMetadata().getIndexManager().getIndexes();
        for (final OIndex index : afterIndexes)
            Assert.assertEquals(index.getSize(), indexSizeMap.get(index.getName()).longValue());
    }

    @Test
    public void testUpdateDocumentWithoutClass() {
        final Collection<? extends OIndex> beforeIndexes = database.getMetadata().getIndexManager().getIndexes();
        final Map<String, Long> indexSizeMap = new HashMap<String, Long>();

        for(final OIndex index : beforeIndexes)
            indexSizeMap.put(index.getName(), index.getSize());

        final ODocument docOne = new ODocument(database);
        docOne.field("prop1", "a");
        docOne.save();

        final ODocument docTwo = new ODocument(database);
        docTwo.field("prop1", "b");
        docTwo.save();

        docOne.field("prop1", "a");
        docOne.save();

        final Collection<? extends OIndex> afterIndexes = database.getMetadata().getIndexManager().getIndexes();
        for (final OIndex index : afterIndexes)
            Assert.assertEquals(index.getSize(), indexSizeMap.get(index.getName()).longValue());
    }

    @Test
    public void testDeleteDocumentWithoutClass() {
        final ODocument docOne = new ODocument(database);
        docOne.field("prop1", "a");
        docOne.save();

        docOne.delete();
    }

    @Test
    public void testDeleteModifiedDocumentWithoutClass() {
        final ODocument docOne = new ODocument(database);
        docOne.field("prop1", "a");
        docOne.save();

        docOne.field("prop1", "b");

        docOne.delete();
    }

    @Test
    public void testDocumentUpdateWithoutDirtyFields() {
        final ODocument docOne = new ODocument(database, "classIndexManagerTestClass");
        docOne.field("prop1", "a");
        docOne.save();

        docOne.setDirty();
        docOne.save();
    }

    @Test
    public void testCreateDocumentIndexRecordAdded() {
        final ODocument doc = new ODocument(database, "classIndexManagerTestClass");
        doc.field("prop0", "x");
        doc.field("prop1", "a");
        doc.field("prop2", 1);

        doc.save();

        final OSchema schema = database.getMetadata().getSchema();
        final OClass oClass = schema.getClass("classIndexManagerTestClass");
        final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");

        final OIndex propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
        Assert.assertNotNull(propOneIndex.get("a"));
        Assert.assertEquals(propOneIndex.getSize(), 1);

        final OIndex compositeIndex = oClass.getClassIndex("classIndexManagerComposite");

        final OIndexDefinition compositeIndexDefinition = compositeIndex.getDefinition();
        Assert.assertNotNull(compositeIndex.get(compositeIndexDefinition.createValue("a", 1)));
        Assert.assertEquals(compositeIndex.getSize(), 1);

        final OIndex propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");
        Assert.assertNotNull(propZeroIndex.get("x"));
        Assert.assertEquals(propZeroIndex.getSize(), 1);
    }

    @Test
    public void testUpdateDocumentIndexRecordRemoved() {
        final ODocument doc = new ODocument(database, "classIndexManagerTestClass");
        doc.field("prop0", "x");
        doc.field("prop1", "a");
        doc.field("prop2", 1);

        doc.save();

        final OSchema schema = database.getMetadata().getSchema();
        final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
        final OClass oClass = schema.getClass("classIndexManagerTestClass");

        final OIndex propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
        final OIndex compositeIndex = oClass.getClassIndex("classIndexManagerComposite");
        final OIndex propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");


        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 1);
        Assert.assertEquals(propZeroIndex.getSize(), 1);

        doc.removeField("prop2");
        doc.removeField("prop0");
        doc.save();


        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 0);
        Assert.assertEquals(propZeroIndex.getSize(), 0);
    }

    @Test
    public void testUpdateDocumentNullKeyIndexRecordRemoved() {
        final ODocument doc = new ODocument(database, "classIndexManagerTestClass");

        doc.field("prop0", "x");
        doc.field("prop1", "a");
        doc.field("prop2", 1);

        doc.save();

        final OSchema schema = database.getMetadata().getSchema();
        final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
        final OClass oClass = schema.getClass("classIndexManagerTestClass");

        final OIndex propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
        final OIndex compositeIndex = oClass.getClassIndex("classIndexManagerComposite");
        final OIndex propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");

        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 1);
        Assert.assertEquals(propZeroIndex.getSize(), 1);

        doc.field("prop2", (Object) null);
        doc.field("prop0", (Object) null);
        doc.save();


        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 0);
        Assert.assertEquals(propZeroIndex.getSize(), 0);
    }


    @Test
    public void testUpdateDocumentIndexRecordUpdated() {
        final ODocument doc = new ODocument(database, "classIndexManagerTestClass");
        doc.field("prop0", "x");
        doc.field("prop1", "a");
        doc.field("prop2", 1);

        doc.save();

        final OSchema schema = database.getMetadata().getSchema();
        final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
        final OClass oClass = schema.getClass("classIndexManagerTestClass");

        final OIndex propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");
        final OIndex propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
        final OIndex compositeIndex = oClass.getClassIndex("classIndexManagerComposite");
        final OIndexDefinition compositeIndexDefinition = compositeIndex.getDefinition();

        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 1);
        Assert.assertEquals(propZeroIndex.getSize(), 1);

        doc.field("prop2", 2);
        doc.field("prop0", "y");
        doc.save();

        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 1);
        Assert.assertEquals(propZeroIndex.getSize(), 1);

        Assert.assertNotNull(propZeroIndex.get("y"));
        Assert.assertNotNull(propOneIndex.get("a"));
        Assert.assertNotNull(compositeIndex.get(compositeIndexDefinition.createValue("a", 2)));
    }

    @Test
    public void testUpdateDocumentIndexRecordUpdatedFromNullField() {
        final ODocument doc = new ODocument(database, "classIndexManagerTestClass");
        doc.field("prop1", "a");
        doc.field("prop2", (Object) null);

        doc.save();

        final OSchema schema = database.getMetadata().getSchema();
        final OClass oClass = schema.getClass("classIndexManagerTestClass");

        final OIndex propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
        final OIndex compositeIndex = oClass.getClassIndex("classIndexManagerComposite");
        final OIndexDefinition compositeIndexDefinition = compositeIndex.getDefinition();

        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 0);

        doc.field("prop2", 2);
        doc.save();

        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 1);

        Assert.assertNotNull(propOneIndex.get("a"));
        Assert.assertNotNull(compositeIndex.get(compositeIndexDefinition.createValue("a", 2)));
    }

    @Test
    public void testDeleteDocumentIndexRecordDeleted() {
        final ODocument doc = new ODocument(database, "classIndexManagerTestClass");
        doc.field("prop0", "x");
        doc.field("prop1", "a");
        doc.field("prop2", 1);

        doc.save();

        final OSchema schema = database.getMetadata().getSchema();
        final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
        final OClass oClass = schema.getClass("classIndexManagerTestClass");

        final OIndex propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");
        final OIndex propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
        final OIndex compositeIndex = oClass.getClassIndex("classIndexManagerComposite");

        Assert.assertEquals(propZeroIndex.getSize(), 1);
        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 1);

        doc.delete();

        Assert.assertEquals(propZeroIndex.getSize(), 0);
        Assert.assertEquals(propOneIndex.getSize(), 0);
        Assert.assertEquals(compositeIndex.getSize(), 0);
    }

    @Test
    public void testDeleteUpdatedDocumentIndexRecordDeleted() {
        final ODocument doc = new ODocument(database, "classIndexManagerTestClass");
        doc.field("prop0", "x");
        doc.field("prop1", "a");
        doc.field("prop2", 1);

        doc.save();

        final OSchema schema = database.getMetadata().getSchema();
        final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
        final OClass oClass = schema.getClass("classIndexManagerTestClass");

        final OIndex propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
        final OIndex compositeIndex = oClass.getClassIndex("classIndexManagerComposite");

        final OIndex propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");
        Assert.assertEquals(propZeroIndex.getSize(), 1);
        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 1);

        doc.field("prop2", 2);
        doc.field("prop0", "y");

        doc.delete();

        Assert.assertEquals(propZeroIndex.getSize(), 0);
        Assert.assertEquals(propOneIndex.getSize(), 0);
        Assert.assertEquals(compositeIndex.getSize(), 0);
    }

    @Test
    public void testDeleteUpdatedDocumentNullFieldIndexRecordDeleted() {
        final ODocument doc = new ODocument(database, "classIndexManagerTestClass");
        doc.field("prop1", "a");
        doc.field("prop2", (Object) null);

        doc.save();

        final OSchema schema = database.getMetadata().getSchema();
        final OClass oClass = schema.getClass("classIndexManagerTestClass");

        final OIndex propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
        final OIndex compositeIndex = oClass.getClassIndex("classIndexManagerComposite");

        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 0);

        doc.delete();

        Assert.assertEquals(propOneIndex.getSize(), 0);
        Assert.assertEquals(compositeIndex.getSize(), 0);
    }

    @Test
    public void testDeleteUpdatedDocumentOrigNullFieldIndexRecordDeleted() {
        final ODocument doc = new ODocument(database, "classIndexManagerTestClass");
        doc.field("prop1", "a");
        doc.field("prop2", (Object) null);

        doc.save();

        final OSchema schema = database.getMetadata().getSchema();
        final OClass oClass = schema.getClass("classIndexManagerTestClass");

        final OIndex propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
        final OIndex compositeIndex = oClass.getClassIndex("classIndexManagerComposite");

        Assert.assertEquals(propOneIndex.getSize(), 1);
        Assert.assertEquals(compositeIndex.getSize(), 0);

        doc.field("prop2", 2);

        doc.delete();

        Assert.assertEquals(propOneIndex.getSize(), 0);
        Assert.assertEquals(compositeIndex.getSize(), 0);
    }

    @Test
    public void testNoClassIndexesUpdate() {
        final ODocument doc = new ODocument(database, "classclassIndexManagerTestClassTwo");
        doc.field("prop1", "a");
        doc.save();

        doc.field("prop1", "b");
        doc.save();

        final OSchema schema = database.getMetadata().getSchema();
        final OClass oClass = schema.getClass("classIndexManagerTestClass");

        final Collection<OIndex<?>> indexes = oClass.getIndexes();
        for (final OIndex index : indexes) {
            Assert.assertEquals(index.getSize(), 0);
        }
    }

    @Test
    public void testNoClassIndexesDelete() {
        final ODocument doc = new ODocument(database, "classIndexManagerTestClassTwo");
        doc.field("prop1", "a");
        doc.save();

        doc.delete();
    }
}
