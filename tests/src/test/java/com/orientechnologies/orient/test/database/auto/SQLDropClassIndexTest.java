package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.*;


@Test(groups = {"index"})
public class SQLDropClassIndexTest {
    private static final OType EXPECTED_PROP1_TYPE = OType.DOUBLE;
    private static final OType EXPECTED_PROP2_TYPE = OType.INTEGER;

    private final ODatabaseDocumentTx database;

    @Parameters(value = "url")
    public SQLDropClassIndexTest(final String iURL) {
        database =  new ODatabaseDocumentTx(iURL);
    }

    @BeforeClass
    public void beforeClass() {
        if(database.isClosed())
            database.open("admin", "admin");

        final OSchema schema = database.getMetadata().getSchema();
        final OClass oClass = schema.createClass("SQLDropClassTestClass");
        oClass.createProperty("prop1", EXPECTED_PROP1_TYPE);
        oClass.createProperty("prop2", EXPECTED_PROP2_TYPE);

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
        database.close();
    }

    @Test
    public void testIndexDeletion() throws Exception {
        database.command(new OCommandSQL("CREATE INDEX SQLDropClassCompositeIndex ON SQLDropClassTestClass (prop1, prop2) UNIQUE")).execute();
        database.getMetadata().getIndexManager().reload();

        Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("SQLDropClassCompositeIndex"));

        database.command(new OCommandSQL("DROP CLASS SQLDropClassTestClass")).execute();
        database.getMetadata().getIndexManager().reload();
        database.getMetadata().getSchema().reload();

        Assert.assertNull(database.getMetadata().getSchema().getClass("SQLDropClassTestClass"));
        Assert.assertNull(database.getMetadata().getIndexManager().getIndex("SQLDropClassCompositeIndex"));
    }
}
