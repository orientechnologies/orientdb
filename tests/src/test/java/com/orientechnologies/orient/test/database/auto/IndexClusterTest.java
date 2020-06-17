package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class IndexClusterTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public IndexClusterTest(@Optional String url) {
    super(url);
  }

  @Test
  public void indexAfterRebuildShouldIncludeAllClusters() {
    // given
    OSchema schema = database.getMetadata().getSchema();
    String className = "IndexClusterTest";

    OClass oclass = schema.createClass(className);
    oclass.createProperty("key", OType.STRING);
    oclass.createProperty("value", OType.INTEGER);
    oclass.createIndex(className + "index1", OClass.INDEX_TYPE.NOTUNIQUE, "key");

    database.<ODocument>newInstance(className).field("key", "a").field("value", 1).save();

    int clId = database.addCluster(className + "secondCluster");
    oclass.addClusterId(clId);

    database
        .<ODocument>newInstance(className)
        .field("key", "a")
        .field("value", 2)
        .save(className + "secondCluster");

    // when
    database.command(new OCommandSQL("rebuild index " + className + "index1")).execute();
    assertEquals(
        database
            .query(new OSQLSynchQuery<Object>("select from " + className + " where key = 'a'"))
            .size(),
        2);
  }
}
