package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;

import static org.testng.Assert.assertEquals;

public class ODatabaseExportTest {

    @Test
    public void exportShouldNotLoseUncommitedIndexEntries() throws Exception {


        ODatabaseDocumentTx tx = new ODatabaseDocumentTx("memory:exportTest").create();

        OClass kl = tx.getMetadata().getSchema().createClass("test");
        kl.createProperty("id", OType.STRING);
        kl.createIndex("id", OClass.INDEX_TYPE.NOTUNIQUE,"id");

        tx.newInstance("test").field("id","a").save();

        assertEquals(tx.getMetadata().getIndexManager().getIndex("id").count("a"), 1);

        new ODatabaseExport(tx, new ByteArrayOutputStream(), new OCommandOutputListener() {
            @Override
            public void onMessage(String iText) {
            }
        }).exportDatabase();

        assertEquals(tx.getMetadata().getIndexManager().getIndex("id").count("a"),1);
        tx.close();

    }

}
