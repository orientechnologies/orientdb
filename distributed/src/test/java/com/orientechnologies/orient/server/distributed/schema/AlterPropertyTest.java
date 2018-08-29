package com.orientechnologies.orient.server.distributed.schema;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AlterPropertyTest extends AbstractServerClusterTest {

    private OProperty property;

    @Test
    public void test() throws Exception {
        init(2);
        prepare(true);
        execute();
    }

    @Override
    protected void onAfterDatabaseCreation(OrientBaseGraph db) {
        OClass oClass = db.getRawGraph().getMetadata().getSchema().createClass("AlterPropertyTestClass");

        property = oClass.createProperty("property", OType.STRING);
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
            testAlterCustomAttributeInProperty();
            testAlterCustomAttributeWithDotInProperty();
        } finally {
            db.close();
        }
    }

    private void testAlterCustomAttributeInProperty() {
        property.setCustom("customAttribute", "value");
        assertEquals("value", property.getCustom("customAttribute"));
    }

    private void testAlterCustomAttributeWithDotInProperty() {
        property.setCustom("custom.attribute", "value");
        assertEquals("value", property.getCustom("custom.attribute"));
    }
}
