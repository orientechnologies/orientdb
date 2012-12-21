package com.orientechnologies.orient.test.database.auto;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Andrey Lomakin
 * @since 14.12.12
 */
@Test
public class TombstonesIterationTest {
  private ODatabaseDocumentTx db;

  @Parameters(value = "url")
  public TombstonesIterationTest(String url) {
    db = new ODatabaseDocumentTx(url);
  }

  @BeforeMethod
  public void beforeMethod() {
    db.open("admin", "admin");

    initSchema();
  }

  @AfterMethod
  public void afterMethod() {
    db.close();
  }

  public void testTombstoneIteration() throws Exception {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    final int docCount = 10000;

    final ODocument document = new ODocument();
    final TreeSet<OClusterPosition> clusterPositions = new TreeSet<OClusterPosition>();
    for (int i = 0; i < docCount; i++) {
      document.reset();
      document.setClassName("Zombie");

      document.field("name", "Flasheater" + i);
      document.field("rank", i);

      document.save();

      clusterPositions.add(document.getIdentity().getClusterPosition());
    }

    final HashSet<OClusterPosition> deletedItems = new HashSet<OClusterPosition>();
    final Random random = new Random();
    ORecordIteratorCluster<ODocument> clusterIterator = db.browseCluster("Zombie", OClusterPosition.INVALID_POSITION,
        OClusterPosition.INVALID_POSITION, true);

    while (clusterIterator.hasNext()) {
      final ODocument doc = clusterIterator.next();

      if (random.nextBoolean()) {
        deletedItems.add(doc.getIdentity().getClusterPosition());
        doc.delete();
      }
    }

    Assert.assertEquals(db.countClass("Zombie"), docCount - deletedItems.size());

    clusterIterator = db.browseCluster("Zombie", OClusterPosition.INVALID_POSITION, OClusterPosition.INVALID_POSITION, true);

    Iterator<OClusterPosition> positionIterator = clusterPositions.iterator();

    while (clusterIterator.hasNext()) {
      final ODocument doc = clusterIterator.next();
      final OClusterPosition position = positionIterator.next();

      Assert.assertEquals(position, doc.getIdentity().getClusterPosition());
      if (deletedItems.contains(position))
        Assert.assertTrue(doc.getRecordVersion().isTombstone());
    }

    Assert.assertFalse(clusterIterator.hasNext());
    Assert.assertFalse(positionIterator.hasNext());

    clusterIterator = db.browseCluster("Zombie");
    positionIterator = clusterPositions.iterator();

    while (positionIterator.hasNext()) {
      final OClusterPosition position = positionIterator.next();
      if (deletedItems.contains(position))
        continue;

      Assert.assertTrue(clusterIterator.hasNext());
      Assert.assertEquals(position, clusterIterator.next().getIdentity().getClusterPosition());
    }

    Assert.assertFalse(clusterIterator.hasNext());
    Assert.assertFalse(positionIterator.hasNext());
  }

  public void testTombstoneIterationInInterval() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    final int docCount = 10000;

    final ODocument document = new ODocument();
    final TreeSet<OClusterPosition> clusterPositions = new TreeSet<OClusterPosition>();
    for (int i = 0; i < docCount; i++) {
      document.reset();
      document.setClassName("Zombie");

      document.field("name", "Flasheater" + i);
      document.field("rank", i);

      document.save();

      clusterPositions.add(document.getIdentity().getClusterPosition());
    }

    final HashSet<OClusterPosition> deletedItems = new HashSet<OClusterPosition>();
    final Random random = new Random();
    random.setSeed(119269911101956L);
    ORecordIteratorCluster<ODocument> clusterIterator = db.browseCluster("Zombie", OClusterPosition.INVALID_POSITION,
        OClusterPosition.INVALID_POSITION, true);

    while (clusterIterator.hasNext()) {
      final ODocument doc = clusterIterator.next();

      if (random.nextBoolean()) {
        deletedItems.add(doc.getIdentity().getClusterPosition());
        doc.delete();
      }
    }

    Assert.assertEquals(db.countClass("Zombie"), docCount - deletedItems.size());

    OClusterPosition startPosition = null;
    OClusterPosition endPosition = null;

    int processedDocs = 0;

    for (OClusterPosition position : clusterPositions) {
      processedDocs++;
      if (processedDocs == 1000) {
        startPosition = position;
      } else if (processedDocs == 2000) {
        endPosition = position;
        break;
      }
    }

    clusterIterator = db.browseCluster("Zombie", startPosition, endPosition, true);
    Iterator<OClusterPosition> positionIterator = clusterPositions.subSet(startPosition, true, endPosition, true).iterator();

    while (clusterIterator.hasNext()) {
      final ODocument doc = clusterIterator.next();
      final OClusterPosition position = positionIterator.next();

      Assert.assertEquals(position, doc.getIdentity().getClusterPosition());
      if (deletedItems.contains(position))
        Assert.assertTrue(doc.getRecordVersion().isTombstone());
    }

    Assert.assertFalse(clusterIterator.hasNext());
    Assert.assertFalse(positionIterator.hasNext());

    clusterIterator = db.browseCluster("Zombie", startPosition, endPosition, false);
    positionIterator = clusterPositions.subSet(startPosition, true, endPosition, true).iterator();

    while (positionIterator.hasNext()) {
      final OClusterPosition position = positionIterator.next();
      if (deletedItems.contains(position))
        continue;

      Assert.assertTrue(clusterIterator.hasNext());
      Assert.assertEquals(position, clusterIterator.next().getIdentity().getClusterPosition());
    }

    Assert.assertFalse(clusterIterator.hasNext());
    Assert.assertFalse(positionIterator.hasNext());
  }

  private void initSchema() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    final OSchema schema = db.getMetadata().getSchema();

    if (schema.existsClass("Zombie"))
      schema.dropClass("Zombie");

    OClass zombieClass = schema.createClass("Zombie");

    zombieClass.createProperty("name", OType.STRING);
    zombieClass.createProperty("rank", OType.INTEGER);

    schema.save();

  }
}
