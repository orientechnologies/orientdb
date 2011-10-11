package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Test(groups = {"index"})
public class SQLSelectCompositeIndexDirectSearchTest {
    private final ODatabaseDocumentTx database;
    private final List<ORID> rids = new ArrayList<ORID>(100);

    @Parameters(value = "url")
    public SQLSelectCompositeIndexDirectSearchTest(final String iURL) {
        database =  new ODatabaseDocumentTx(iURL);
    }


    @BeforeClass
    public void beforeClass() throws Exception {
        database.open("admin", "admin");

        final OSchema schema = database.getMetadata().getSchema();

        final OClass testClass = schema.createClass("SQLSelectCompositeIndexDirectSearchTestClass");
        testClass.createProperty("prop1", OType.INTEGER);
        testClass.createProperty("prop2", OType.INTEGER);
        schema.save();

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                final ODocument document = new ODocument(database, "SQLSelectCompositeIndexDirectSearchTestClass");
                document.field("prop1", i);
                document.field("prop2", j);
                document.save();
                rids.add(document.getRecord().getIdentity());
            }
        }

        database.command(new OCommandSQL("create index SQLSelectCompositeIndexDirectSearchTestIndex on SQLSelectCompositeIndexDirectSearchTestClass (prop1, prop2) NOTUNIQUE")).execute();

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

    @Test
    public void testEquals() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key = [2, 5]")).execute();
        Assert.assertEquals(resultList.size(), 1);
        final ODocument result = resultList.get(0);
        Assert.assertTrue(result.containsField("key"));
        Assert.assertTrue(result.containsField("rid"));
        Assert.assertEquals(result.<OIdentifiable>field("rid").getIdentity(), rids.get(25));
    }

    @Test
    public void testBetween() throws Exception {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key between [2, 5] and [5, 6]")).execute();
        Assert.assertEquals(resultList.size(), 32);

        final List<ORID> expectedRIDs = rids.subList(25, 57);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMajor() throws Exception {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key > [2, 5]")).execute();
        Assert.assertEquals(resultList.size(), 74);

        final List<ORID> expectedRIDs = rids.subList(26, 100);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMinor() throws Exception {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key < [2, 5]")).execute();
        Assert.assertEquals(resultList.size(), 25);

        final List<ORID> expectedRIDs = rids.subList(0, 26);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMajorEquals() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key >= [2, 5]")).execute();
        Assert.assertEquals(resultList.size(), 75);

        final List<ORID> expectedRIDs = rids.subList(25, 100);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMinorEquals() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key <= [2, 5]")).execute();
        Assert.assertEquals(resultList.size(), 26);

        final List<ORID> expectedRIDs = rids.subList(0, 26);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test(enabled = false)
    public void testIn() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key in [[2, 5], [3, 4], [7, 7]]")).execute();
        Assert.assertEquals(resultList.size(), 3);

        final List<ORID> expectedRIDs = Arrays.asList(rids.get(25), rids.get(34), rids.get(77));
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testEqualsPartialPrimitive() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key = 2")).execute();
        Assert.assertEquals(resultList.size(), 10);

        final List<ORID> expectedRIDs = rids.subList(20, 30);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testEqualsPartialComposite() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key = [2]")).execute();
        Assert.assertEquals(resultList.size(), 10);

        final List<ORID> expectedRIDs = rids.subList(20, 30);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testBetweenPartialPrimitive() throws Exception {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key between 2 and 5")).execute();
        Assert.assertEquals(resultList.size(), 40);

        final List<ORID> expectedRIDs = rids.subList(20, 60);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testBetweenPartialComposite() throws Exception {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key between [2] and [5]")).execute();
        Assert.assertEquals(resultList.size(), 40);

        final List<ORID> expectedRIDs = rids.subList(20, 60);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMajorPartialPrimitive() throws Exception {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key > 2")).execute();
        Assert.assertEquals(resultList.size(), 70);

        final List<ORID> expectedRIDs = rids.subList(30, 100);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMajorPartialComposite() throws Exception {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key > [2]")).execute();
        Assert.assertEquals(resultList.size(), 70);

        final List<ORID> expectedRIDs = rids.subList(30, 100);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMinorPartialPrimitive() throws Exception {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key < 5")).execute();
        Assert.assertEquals(resultList.size(), 50);

        final List<ORID> expectedRIDs = rids.subList(0, 50);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMinorPartialComposite() throws Exception {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key < [5]")).execute();
        Assert.assertEquals(resultList.size(), 50);

        final List<ORID> expectedRIDs = rids.subList(0, 50);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMajorEqualsPartialPrimitive() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key >= 5")).execute();
        Assert.assertEquals(resultList.size(), 50);

        final List<ORID> expectedRIDs = rids.subList(50, 100);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMajorEqualsPartialComposite() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key >= [5]")).execute();
        Assert.assertEquals(resultList.size(), 50);

        final List<ORID> expectedRIDs = rids.subList(50, 100);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMinorEqualsPartialPrimitive() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key <= 4")).execute();
        Assert.assertEquals(resultList.size(), 50);

        final List<ORID> expectedRIDs = rids.subList(0, 50);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test
    public void testMinorEqualsPartialComposite() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key <= [4]")).execute();
        Assert.assertEquals(resultList.size(), 50);

        final List<ORID> expectedRIDs = rids.subList(0, 50);
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test(enabled = false)
    public void testInPartialPrimitive() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key in [2, 3, 7]")).execute();
        Assert.assertEquals(resultList.size(), 30);

        final List<ORID> expectedRIDs = rids.subList(20, 40);
        expectedRIDs.addAll(rids.subList(70, 80));
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }

    @Test(enabled = false)
    public void testInPartialComposite() {
        final List<ODocument> resultList = database.command(new OCommandSQL("select * from index:SQLSelectCompositeIndexDirectSearchTestIndex where key in [[2], [3], [7]]")).execute();
        Assert.assertEquals(resultList.size(), 30);

        final List<ORID> expectedRIDs = rids.subList(20, 40);
        expectedRIDs.addAll(rids.subList(70, 80));
        for (final ODocument d : resultList) {
            Assert.assertTrue(d.containsField("rid"));

            Assert.assertTrue(expectedRIDs.contains(d.<ORID>field("rid")));
        }
    }
}
