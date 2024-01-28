package com.orientechnologies.orient.core.sql.executor;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila
 */
public class ORebuildIndexStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void indexAfterRebuildShouldIncludeAllClusters() {
    // given
    OSchema schema = db.getMetadata().getSchema();
    String className = "IndexClusterTest";

    OClass oclass = schema.createClass(className);
    oclass.createProperty("key", OType.STRING);
    oclass.createProperty("value", OType.INTEGER);
    oclass.createIndex(className + "index1", OClass.INDEX_TYPE.NOTUNIQUE, "key");

    OElement ele = db.newInstance(className);
    ele.setProperty("key", "a");
    ele.setProperty("value", 1);
    db.save(ele);

    int clId = db.addCluster(className + "secondCluster");
    oclass.addClusterId(clId);

    OElement ele1 = db.newInstance(className);
    ele1.setProperty("key", "a");
    ele1.setProperty("value", 2);
    db.save(ele1, className + "secondCluster");

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
  }
}
