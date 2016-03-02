package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;

@Test
public class TruncateClusterTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public TruncateClusterTest(@Optional String url) {
    super(url);
  }

  public void testSimpleCluster() {
    final String clusterName = "TruncateCluster";

    final int clusterId = database.addCluster(clusterName);
    final ODocument document = new ODocument();
    document.save(clusterName);

    Assert.assertEquals(database.countClusterElements(clusterId), 1);

    database.truncateCluster(clusterName);

    Assert.assertEquals(database.countClusterElements(clusterId), 0);

    database.dropCluster(clusterId, false);
  }

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

    List<ODocument> indexQuery = database
        .query(new OSQLSynchQuery<ODocument>("select from TruncateClusterClass where value='val'"));
    Assert.assertEquals(indexQuery.size(), 1);

    database.truncateCluster(clusterName);

    Assert.assertEquals(database.countClass(className), 0);
    Assert.assertEquals(database.countClusterElements(clusterId), 0);

    indexQuery = database.query(new OSQLSynchQuery<ODocument>("select from TruncateClusterClass where value='val'"));

    Assert.assertEquals(indexQuery.size(), 0);
  }

  public void testSimpleClusterIsAbsent() {
    final String clusterName = "TruncateClusterIsAbsent";
    final int clusterId = database.addCluster(clusterName);

    final ODocument document = new ODocument();
    document.save(clusterName);

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    try {
      database.truncateCluster("Wrong" + clusterName);

      Assert.fail();
    } catch (OException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    database.dropCluster(clusterId, false);
  }

  public void testClusterInClassIsAbsent() {
    final String clusterName = "TruncateClusterInClassIsAbsent";

    final int clusterId = database.addCluster(clusterName);

    final String className = "TruncateClusterIsAbsentClass";
    final OSchema schema = database.getMetadata().getSchema();

    final OClass clazz = schema.createClass(className);
    clazz.addClusterId(clusterId);

    final ODocument document = new ODocument();
    document.save(clusterName);

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    Assert.assertEquals(database.countClass(className), 1);

    try {
      clazz.truncateCluster("Wrong" + clusterName);
      Assert.fail();
    } catch (OException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(database.countClusterElements(clusterId), 1);
    Assert.assertEquals(database.countClass(className), 1);

    database.dropCluster(clusterId, false);
  }
}
