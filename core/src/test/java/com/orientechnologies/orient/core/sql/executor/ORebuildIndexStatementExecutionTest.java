package com.orientechnologies.orient.core.sql.executor;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.junit.Test;

/** @author Luigi Dell'Aquila */
public class ORebuildIndexStatementExecutionTest {

  @Test
  public void indexAfterRebuildShouldIncludeAllClusters() {
    // given
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:ORebuildIndexStatementExecutionTest");
    db.create();
    try {
      OSchema schema = db.getMetadata().getSchema();
      String className = "IndexClusterTest";

      OClass oclass = schema.createClass(className);
      oclass.createProperty("key", OType.STRING);
      oclass.createProperty("value", OType.INTEGER);
      oclass.createIndex(className + "index1", OClass.INDEX_TYPE.NOTUNIQUE, "key");

      db.newInstance(className).field("key", "a").field("value", 1).save();

      int clId = db.addCluster(className + "secondCluster");
      oclass.addClusterId(clId);

      db.newInstance(className)
          .field("key", "a")
          .field("value", 2)
          .save(className + "secondCluster");

      // when
      OResultSet result = db.command("rebuild index " + className + "index1");
      Assert.assertTrue(result.hasNext());
      OResult resultRecord = result.next();
      Assert.assertEquals(resultRecord.<Object>getProperty("totalIndexed"), 2l);
      Assert.assertFalse(result.hasNext());
      assertEquals(
          db.query(new OSQLSynchQuery<Object>("select from " + className + " where key = 'a'"))
              .size(),
          2);
    } finally {
      db.drop();
    }
  }
}
