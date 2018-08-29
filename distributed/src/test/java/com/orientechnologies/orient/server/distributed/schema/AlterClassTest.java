package com.orientechnologies.orient.server.distributed.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AlterClassTest extends AbstractServerClusterTest {

    private OClass oClass;

    @Test
    public void test() throws Exception {
        init(2);
        prepare(true);
        execute();
    }

    @Override
    protected void onAfterDatabaseCreation(OrientBaseGraph db) {
        oClass = db.getRawGraph().getMetadata().getSchema().createClass("AlterPropertyTestClass");
    }

    @Override
    protected String getDatabaseName() {
        return "testdb";
    }

    @Override
    protected String getDatabaseURL(ServerRun server) {
        return "plocal:" + server.getDatabasePath(getDatabaseName());
    }

    @Override
    protected void executeTest() throws Exception {
        ODatabaseDocument db = new ODatabaseDocumentTx(getDatabaseURL(serverInstance.get(0)));
        db.open("admin", "admin");
        try {
            testAlterCustomAttributeInClass();
            testAlterCustomAttributeWithDotInClass();
        } finally {
            db.close();
        }
    }

    private void testAlterCustomAttributeInClass() {
        oClass.setCustom("customAttribute", "value");
        assertEquals("value", oClass.getCustom("customAttribute"));
    }

    private void testAlterCustomAttributeWithDotInClass() {
        oClass.setCustom("custom.attribute", "value");
        assertEquals("value", oClass.getCustom("custom.attribute"));
    }
}
