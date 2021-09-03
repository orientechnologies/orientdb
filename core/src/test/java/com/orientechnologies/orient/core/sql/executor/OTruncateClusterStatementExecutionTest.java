package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OTruncateClusterStatementExecutionTest {
  static ODatabaseDocument database;

  @BeforeClass
  public static void beforeClass() {
    database = new ODatabaseDocumentTx("memory:OTruncateClusterStatementExecutionTest");
    database.create();
  }

  @AfterClass
  public static void afterClass() {
    database.close();
  }

  @Test
  public void testClusterWithIndex() {
    final String clusterName = "TruncateClusterWithIndex";
    final int clusterId = database.addCluster(clusterName);

    final String className = "TruncateClusterClass";
    final OSchema schema = database.getMetadata().getSchema();

    final OClass clazz = schema.createClass(className);
    clazz.addClusterId(clusterId);

    clazz.createProperty("value", OType.STRING);
    clazz.createIndex("TruncateClusterIndex", OClass.INDEX_TYPE.UNIQUE, "value");

    final ODocument document = new ODocument();
    document.field("value", "val");

    document.save(clusterName);

    Assert.assertEquals(database.countClass(className), 1);
    Assert.assertEquals(database.countClusterElements(clusterId), 1);

    OResultSet indexQuery = database.query("select from TruncateClusterClass where value='val'");
    Assert.assertEquals(toList(indexQuery).size(), 1);
    indexQuery.close();

    database.command("truncate cluster " + clusterName);

    Assert.assertEquals(database.countClass(className), 0);
    Assert.assertEquals(database.countClusterElements(clusterId), 0);

    indexQuery = database.query("select from TruncateClusterClass where value='val'");

    Assert.assertEquals(toList(indexQuery).size(), 0);
    indexQuery.close();
  }

  private List<OResult> toList(OResultSet input) {
    List<OResult> result = new ArrayList<>();
    while (input.hasNext()) {
      result.add(input.next());
    }
    return result;
  }
}
