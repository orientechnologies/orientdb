package com.orientechnologies.test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Created by enricorisa on 03/04/14.
 */
public class TestOrient {

  public static void main(String[] args) {

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/TESTDB").open("admin", "admin");
    final OSchema schema = db.getMetadata().getSchema();
    final String SUFFIX = "TESTCLUSTER1";

    ODatabaseRecordThreadLocal.INSTANCE.set(db);
    db.declareIntent(new OIntentMassiveInsert());

    OClass testClass1 = schema.createClass("TestClass1");
    OClass testClass2 = schema.createClass("TestClass2");
    OClass testClass3 = schema.createClass("TestClass3");

    testClass2.createProperty("testClass1Property", OType.EMBEDDED, testClass1);

    int clusterId;
    clusterId = db.addCluster("TestClass1" + SUFFIX, OStorage.CLUSTER_TYPE.PHYSICAL);
    schema.getClass("TestClass1").addClusterId(clusterId);
    clusterId = db.addCluster("TestClass2" + SUFFIX, OStorage.CLUSTER_TYPE.PHYSICAL);
    schema.getClass("TestClass2").addClusterId(clusterId);

    // Reassign Classes because I just added clusters
    testClass1 = schema.getClass("TestClass1");
    testClass2 = schema.getClass("TestClass2");

    ODocument testClass2Document = new ODocument(testClass2);
    testClass2Document.field("testClass1Property", new ODocument(testClass1));
    testClass2Document.save("TestClass2" + SUFFIX);

    db.commit();
    db.close();
  }
}
