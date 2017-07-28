package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;
import com.orientechnologies.orient.core.sql.parser.OIndexName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CountFromIndexStepTest {

    private static ODatabaseDocument database;
    private static final String CLASS_NAME = "TestClass";
    private static final String PROPERTY_NAME = "testProperty";
    private static final String INDEX_NAME = CLASS_NAME + "." + PROPERTY_NAME;
    private static final String ALIAS = "size";

    @BeforeClass
    public static void beforeClass() {
        database = new ODatabaseDocumentTx("memory:CountFromIndexStepTest");
        database.create();
    }

    @Test
    public void shouldCountRecordsOfIndex() {
        OClass clazz = database.getMetadata().getSchema().createClass(CLASS_NAME);
        clazz.createProperty(PROPERTY_NAME, OType.STRING);
        clazz.createIndex(INDEX_NAME, OClass.INDEX_TYPE.NOTUNIQUE, PROPERTY_NAME);

        for (int i = 0; i < 20; i++) {
            ODocument document = new ODocument(CLASS_NAME);
            document.field(PROPERTY_NAME, "TestProperty");
            document.save();
        }

        OIndexName name = new OIndexName(-1);
        name.setValue(INDEX_NAME);
        OIndexIdentifier identifier = new OIndexIdentifier(-1);
        identifier.setIndexName(name);
        identifier.setIndexNameString(name.getValue());
        identifier.setType(OIndexIdentifier.Type.INDEX);

        OBasicCommandContext context = new OBasicCommandContext();
        context.setDatabase(database);
        CountFromIndexStep step = new CountFromIndexStep(identifier, ALIAS, context, false);

        OResultSet result = step.syncPull(context, 20);
        Assert.assertEquals(20, (long) result.next().getProperty(ALIAS));
    }

}
