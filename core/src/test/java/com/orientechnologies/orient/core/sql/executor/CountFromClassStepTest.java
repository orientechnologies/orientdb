package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CountFromClassStepTest {

    private static ODatabaseDocument database;
    private static final String CLASS_NAME = "TestClass";
    private static final String ALIAS = "size";

    @BeforeClass
    public static void beforeClass() {
        database = new ODatabaseDocumentTx("memory:CountFromClassStepTest");
        database.create();
    }

    @Test
    public void shouldCountRecordsOfClass() {
        database.getMetadata().getSchema().createClass(CLASS_NAME);
        for (int i = 0; i < 20; i++) {
            ODocument document = new ODocument(CLASS_NAME);
            document.save();
        }

        OIdentifier className = new OIdentifier(-1);
        className.setValue(CLASS_NAME);

        OBasicCommandContext context = new OBasicCommandContext();
        context.setDatabase(database);
        CountFromClassStep step = new CountFromClassStep(className, ALIAS, context, false);

        OResultSet result = step.syncPull(context, 20);
        Assert.assertEquals(20, (long) result.next().getProperty(ALIAS));
    }

}
