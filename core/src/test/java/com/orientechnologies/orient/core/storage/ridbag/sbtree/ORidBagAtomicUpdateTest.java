package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ORidBagAtomicUpdateTest extends BaseMemoryDatabase {
  private int topThreshold;
  private int bottomThreshold;

  @Before
  public void setUp() throws Exception {
    db.declareIntent(new OIntentMassiveInsert());
  }

  @Before
  public void beforeMethod() {
    topThreshold =
        OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
  }

  @After
  public void afterMethod() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }

  @Test
  public void testAddTwoNewDocuments() {
    db.begin();
    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 0);
  }

  @Test
  public void testAddTwoNewDocumentsWithCME() {
    final ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();
    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.getLocalCache().clear();
    ODocument staleCMEDoc = db.load(cmeDoc.getIdentity());
    Assert.assertNotSame(staleCMEDoc, cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    staleCMEDoc.field("v", "v1");
    staleCMEDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 0);
  }

  @Test
  public void testAddTwoAdditionalNewDocuments() {
    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.begin();

    ODocument docThree = new ODocument();
    ODocument docFour = new ODocument();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddingDocsDontUpdateVersion() {
    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();

    ridBag.add(docOne);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    final int version = rootDoc.getVersion();

    ODocument docTwo = new ODocument();
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);

    rootDoc = (ODocument) rootDoc.reload();

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);
  }

  @Test
  public void testAddingDocsDontUpdateVersionInTx() {
    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();

    ridBag.add(docOne);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    final int version = rootDoc.getVersion();

    ODocument docTwo = new ODocument();
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);

    rootDoc = (ODocument) rootDoc.reload();

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);
  }

  @Test
  public void testAddTwoAdditionalNewDocumentsWithCME() {
    final ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.getLocalCache().clear();
    ODocument staleCMEDoc = db.load(cmeDoc.getIdentity());

    Assert.assertNotSame(staleCMEDoc, cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();

    ODocument docThree = new ODocument();
    ODocument docFour = new ODocument();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    staleCMEDoc.field("v", "v1");
    staleCMEDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddTwoSavedDocuments() {
    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));
    ODocument docTwo = new ODocument();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
  }

  @Test
  public void testAddTwoAdditionalSavedDocuments() {
    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.begin();

    ODocument docThree = new ODocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));
    ODocument docFour = new ODocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddTwoAdditionalSavedDocumentsWithCME() {
    final ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.getLocalCache().clear();
    ODocument staleCMEDoc = db.load(cmeDoc.getIdentity());

    Assert.assertNotSame(staleCMEDoc, cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();

    ODocument docThree = new ODocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));
    ODocument docFour = new ODocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    staleCMEDoc.field("v", "vn");
    staleCMEDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddInternalDocumentsAndSubDocuments() {
    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docTwo = new ODocument();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());
    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.begin();

    ODocument docThree = new ODocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFour = new ODocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docThreeOne = new ODocument();
    docThreeOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docThreeTwo = new ODocument();
    docThreeTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagThree = new ORidBag();
    ridBagThree.add(docThreeOne);
    ridBagThree.add(docThreeTwo);
    docThree.field("ridBag", ridBagThree);

    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFourOne = new ODocument();
    docFourOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFourTwo = new ODocument();
    docFourTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagFour = new ORidBag();
    ridBagFour.add(docFourOne);
    ridBagFour.add(docFourTwo);

    docFour.field("ridBag", ridBagFour);

    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddInternalDocumentsAndSubDocumentsWithCME() {
    final ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docTwo = new ODocument();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());
    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.getLocalCache().clear();
    ODocument staleCMEDoc = db.load(cmeDoc.getIdentity());
    Assert.assertNotSame(staleCMEDoc, cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();

    ODocument docThree = new ODocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFour = new ODocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docThreeOne = new ODocument();
    docThreeOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docThreeTwo = new ODocument();
    docThreeTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagThree = new ORidBag();
    ridBagThree.add(docThreeOne);
    ridBagThree.add(docThreeTwo);
    docThree.field("ridBag", ridBagThree);

    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFourOne = new ODocument();
    docFourOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFourTwo = new ODocument();
    docFourTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ORidBag ridBagFour = new ORidBag();
    ridBagFour.add(docFourOne);
    ridBagFour.add(docFourTwo);

    docFour.field("ridBag", ridBagFour);

    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    staleCMEDoc.field("v", "vn");
    staleCMEDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  /** This test is no longer useful */
  @Test
  @Ignore
  public void testAddTwoSavedDocumentsWithoutTx() {
    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument staleRooDoc = db.load(rootDoc.getIdentity());
    ORidBag staleRidBag = staleRooDoc.field("ridBag");

    staleRidBag.add(docOne);
    staleRidBag.add(docTwo);

    rootDoc.setDirty();
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    try {
      staleRooDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 0);
  }

  /** This test is no longer useful */
  @Test
  @Ignore
  public void testAddOneSavedDocumentsAndDeleteOneWithoutTx() {
    ODocument docOne = new ODocument();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docTwo = new ODocument();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docThree = new ODocument();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument staleRooDoc = db.load(rootDoc.getIdentity());
    ORidBag staleRidBag = staleRooDoc.field("ridBag");

    Iterator<OIdentifiable> iterator = staleRidBag.iterator();
    iterator.next();
    iterator.remove();

    staleRidBag.add(docThree);

    rootDoc.setDirty();
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    try {
      staleRooDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    iterator = ridBag.iterator();
    Assert.assertEquals(iterator.next(), docOne);
    Assert.assertEquals(iterator.next(), docTwo);

    Assert.assertTrue(!iterator.hasNext());
  }

  @Test
  public void testRandomModificationsNotTx() {
    final long seed = System.currentTimeMillis();
    System.out.println("testRandomModificationsNotTx seed: " + seed);

    final Random rnd = new Random(seed);
    final int iterations = 20;
    for (int n = 0; n < iterations; n++) {
      final int amountOfAddedDocs = rnd.nextInt(10) + 10;
      final int amountOfAddedDocsAfterSave = rnd.nextInt(5) + 5;
      final int amountOfDeletedDocs = rnd.nextInt(5) + 5;

      List<OIdentifiable> addedDocuments = new ArrayList<OIdentifiable>();

      ODocument document = new ODocument();
      ORidBag ridBag = new ORidBag();
      document.field("ridBag", ridBag);

      for (int i = 0; i < amountOfAddedDocs; i++) {
        final ODocument docToAdd = new ODocument();
        docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
        addedDocuments.add(docToAdd.getIdentity());
        ridBag.add(docToAdd);
      }

      document.save(db.getClusterNameById(db.getDefaultClusterId()));

      db.getLocalCache().clear();
      ODocument staleDocument = db.load(document.getIdentity());
      ORidBag staleRidBag = staleDocument.field("ridBag");

      Assert.assertNotSame(document, staleDocument);
      Assert.assertNotSame(ridBag, staleRidBag);

      int k = 0;
      Iterator<OIdentifiable> iterator = staleRidBag.iterator();
      while (k < amountOfDeletedDocs && iterator.hasNext()) {
        iterator.next();
        if (rnd.nextBoolean()) {
          iterator.remove();
          k++;
        }

        if (!iterator.hasNext()) iterator = staleRidBag.iterator();
      }

      for (k = 0; k < amountOfAddedDocsAfterSave; k++) {
        ODocument docToAdd = new ODocument();
        docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));

        staleRidBag.add(docToAdd);
        addedDocuments.add(docToAdd);
      }

      Assert.assertEquals(staleRidBag.size(), addedDocuments.size() - amountOfDeletedDocs);

      document.setDirty();
      document.save(db.getClusterNameById(db.getDefaultClusterId()));
      staleDocument.save(db.getClusterNameById(db.getDefaultClusterId()));

      document = db.load(document.getIdentity());
      ridBag = document.field("ridBag");

      Assert.assertEquals(ridBag.size(), addedDocuments.size() - amountOfDeletedDocs);
      iterator = ridBag.iterator();
      while (iterator.hasNext()) Assert.assertTrue(addedDocuments.contains(iterator.next()));
    }
  }

  @Test
  public void testVisibility() {
    ODocument document = new ODocument();
    ORidBag ridBag = new ORidBag();

    document.field("ridBag", ridBag);
    ODocument docOne = new ODocument();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docTwo = new ODocument();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument copyOne = db.load(document.getIdentity());

    db.getLocalCache().clear();
    ODocument copyTwo = db.load(document.getIdentity());

    Assert.assertNotSame(copyOne, copyTwo);

    ORidBag ridBagOne = copyOne.field("ridBag");
    ORidBag ridBagTwo = copyTwo.field("ridBag");

    ODocument docTree = new ODocument();
    docTree.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument docFour = new ODocument();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBagOne.remove(docOne);

    ridBagOne.add(docTree);
    ridBagTwo.add(docFour);

    Assert.assertEquals(ridBagOne.size(), 2);
    Assert.assertEquals(ridBagTwo.size(), 3);
  }

  @Test
  public void testRandomChangedInTxLevel2() {
    testRandomChangedInTx(2);
  }

  @Test
  public void testRandomChangedInTxLevel1() {
    testRandomChangedInTx(1);
  }

  private void testRandomChangedInTx(final int levels) {
    Random rnd = new Random();

    final List<Integer> amountOfAddedDocsPerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfAddedDocsAfterSavePerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfDeletedDocsPerLevel = new ArrayList<Integer>();
    Map<LevelKey, List<OIdentifiable>> addedDocPerLevel =
        new HashMap<LevelKey, List<OIdentifiable>>();

    for (int i = 0; i < levels; i++) {
      amountOfAddedDocsPerLevel.add(rnd.nextInt(5) + 10);
      amountOfAddedDocsAfterSavePerLevel.add(rnd.nextInt(5) + 5);
      amountOfDeletedDocsPerLevel.add(rnd.nextInt(5) + 5);
    }

    db.begin();
    ODocument rootDoc = new ODocument();
    createDocsForLevel(amountOfAddedDocsPerLevel, 0, levels, addedDocPerLevel, rootDoc);
    db.commit();

    addedDocPerLevel = new HashMap<LevelKey, List<OIdentifiable>>(addedDocPerLevel);

    rootDoc = db.load(rootDoc.getIdentity());
    db.begin();
    deleteDocsForLevel(amountOfDeletedDocsPerLevel, 0, levels, rootDoc, rnd);
    addDocsForLevel(amountOfAddedDocsAfterSavePerLevel, 0, levels, rootDoc);
    db.rollback();

    rootDoc = db.load(rootDoc.getIdentity());
    assertDocsAfterRollback(0, levels, addedDocPerLevel, rootDoc);
  }

  @Test
  public void testRandomChangedInTxWithCME() {
    final ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    Random rnd = new Random();

    final int levels = rnd.nextInt(2) + 1;
    final List<Integer> amountOfAddedDocsPerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfAddedDocsAfterSavePerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfDeletedDocsPerLevel = new ArrayList<Integer>();
    Map<LevelKey, List<OIdentifiable>> addedDocPerLevel =
        new HashMap<LevelKey, List<OIdentifiable>>();

    for (int i = 0; i < levels; i++) {
      amountOfAddedDocsPerLevel.add(rnd.nextInt(5) + 10);
      amountOfAddedDocsAfterSavePerLevel.add(rnd.nextInt(5) + 5);
      amountOfDeletedDocsPerLevel.add(rnd.nextInt(5) + 5);
    }

    db.getLocalCache().clear();
    ODocument staleCMEDoc = db.load(cmeDoc.getIdentity());
    Assert.assertNotSame(staleCMEDoc, cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();
    ODocument rootDoc = new ODocument();
    createDocsForLevel(amountOfAddedDocsPerLevel, 0, levels, addedDocPerLevel, rootDoc);
    db.commit();

    addedDocPerLevel = new HashMap<LevelKey, List<OIdentifiable>>(addedDocPerLevel);

    rootDoc = db.load(rootDoc.getIdentity());
    db.begin();
    deleteDocsForLevel(amountOfDeletedDocsPerLevel, 0, levels, rootDoc, rnd);
    addDocsForLevel(amountOfAddedDocsAfterSavePerLevel, 0, levels, rootDoc);

    staleCMEDoc.field("v", "vn");
    staleCMEDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    assertDocsAfterRollback(0, levels, addedDocPerLevel, rootDoc);
  }

  @Test
  public void testFromEmbeddedToSBTreeRollback() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();

    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    db.begin();
    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.rollback();

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (OIdentifiable identifiable : ridBag) Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromEmbeddedToSBTreeTXWithCME() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    ODocument cmeDocument = new ODocument();
    cmeDocument.field("v", "v1");
    cmeDocument.save(db.getClusterNameById(db.getDefaultClusterId()));

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();

    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    db.getLocalCache().clear();
    ODocument staleDocument = db.load(cmeDocument.getIdentity());
    Assert.assertNotSame(staleDocument, cmeDocument);

    cmeDocument.field("v", "v234");
    cmeDocument.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();
    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    staleDocument.field("v", "ver");
    staleDocument.save(db.getClusterNameById(db.getDefaultClusterId()));

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (OIdentifiable identifiable : ridBag) Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromEmbeddedToSBTreeWithCME() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    db.getLocalCache().clear();
    ODocument cmeDocument = db.load(document.getIdentity());
    Assert.assertNotSame(cmeDocument, document);
    cmeDocument.field("v", "v1");
    cmeDocument.save(db.getClusterNameById(db.getDefaultClusterId()));

    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    try {
      document.save(db.getClusterNameById(db.getDefaultClusterId()));
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (OIdentifiable identifiable : ridBag) Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromSBTreeToEmbeddedRollback() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 10);
    Assert.assertTrue(!ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    db.begin();
    for (int i = 0; i < 4; i++) {
      OIdentifiable docToRemove = docsToAdd.get(i);
      ridBag.remove(docToRemove);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.rollback();

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(!ridBag.isEmbedded());

    for (OIdentifiable identifiable : ridBag) Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromSBTreeToEmbeddedTxWithCME() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    ODocument cmeDoc = new ODocument();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 10);
    Assert.assertTrue(!ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    db.getLocalCache().clear();
    ODocument staleDoc = db.load(cmeDoc.getIdentity());
    Assert.assertNotSame(staleDoc, cmeDoc);

    cmeDoc.field("v", "sd");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.begin();
    for (int i = 0; i < 4; i++) {
      OIdentifiable docToRemove = docsToAdd.get(i);
      ridBag.remove(docToRemove);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    staleDoc.field("v", "d");
    staleDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(!ridBag.isEmbedded());

    for (OIdentifiable identifiable : ridBag) Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  /** This test is no longer useful */
  @Test
  @Ignore
  public void testFromSBTreeToEmbeddedWithCME() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    Assert.assertEquals(docsToAdd.size(), 10);
    Assert.assertTrue(!ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    ODocument cmeDoc = db.load(document.getIdentity());
    cmeDoc.field("v", "v1");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    for (int i = 0; i < 4; i++) {
      OIdentifiable docToRemove = docsToAdd.get(i);
      ridBag.remove(docToRemove);
    }

    Assert.assertTrue(document.isDirty());

    try {
      document.save(db.getClusterNameById(db.getDefaultClusterId()));
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(!ridBag.isEmbedded());

    for (OIdentifiable identifiable : ridBag) Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  private void createDocsForLevel(
      final List<Integer> amountOfAddedDocsPerLevel,
      int level,
      int levels,
      Map<LevelKey, List<OIdentifiable>> addedDocPerLevel,
      ODocument rootDoc) {

    int docs = amountOfAddedDocsPerLevel.get(level);

    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>();
    addedDocPerLevel.put(new LevelKey(rootDoc.getIdentity(), level), addedDocs);

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    for (int i = 0; i < docs; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));

      addedDocs.add(docToAdd.getIdentity());
      ridBag.add(docToAdd);

      if (level + 1 < levels)
        createDocsForLevel(
            amountOfAddedDocsPerLevel, level + 1, levels, addedDocPerLevel, docToAdd);
    }

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  private void deleteDocsForLevel(
      List<Integer> amountOfDeletedDocsPerLevel,
      int level,
      int levels,
      ODocument rootDoc,
      Random rnd) {
    ORidBag ridBag = rootDoc.field("ridBag");
    Iterator<OIdentifiable> iter = ridBag.iterator();
    while (iter.hasNext()) {
      OIdentifiable identifiable = iter.next();
      ODocument doc = identifiable.getRecord();
      if (level + 1 < levels)
        deleteDocsForLevel(amountOfDeletedDocsPerLevel, level + 1, levels, doc, rnd);
    }

    int docs = amountOfDeletedDocsPerLevel.get(level);

    int k = 0;
    Iterator<OIdentifiable> iterator = ridBag.iterator();
    while (k < docs && iterator.hasNext()) {
      iterator.next();

      if (rnd.nextBoolean()) {
        iterator.remove();
        k++;
      }

      if (!iterator.hasNext()) iterator = ridBag.iterator();
    }
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  private void addDocsForLevel(
      List<Integer> amountOfAddedDocsAfterSavePerLevel, int level, int levels, ODocument rootDoc) {
    ORidBag ridBag = rootDoc.field("ridBag");

    for (OIdentifiable identifiable : ridBag) {
      ODocument doc = identifiable.getRecord();
      if (level + 1 < levels)
        addDocsForLevel(amountOfAddedDocsAfterSavePerLevel, level + 1, levels, doc);
    }

    int docs = amountOfAddedDocsAfterSavePerLevel.get(level);
    for (int i = 0; i < docs; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));

      ridBag.add(docToAdd);
    }
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  private void assertDocsAfterRollback(
      int level,
      int levels,
      Map<LevelKey, List<OIdentifiable>> addedDocPerLevel,
      ODocument rootDoc) {
    ORidBag ridBag = rootDoc.field("ridBag");
    List<OIdentifiable> addedDocs =
        new ArrayList<OIdentifiable>(
            addedDocPerLevel.get(new LevelKey(rootDoc.getIdentity(), level)));

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    while (iterator.hasNext()) {
      ODocument doc = iterator.next().getRecord();
      if (level + 1 < levels) assertDocsAfterRollback(level + 1, levels, addedDocPerLevel, doc);
      else Assert.assertNull(doc.field("ridBag"));

      Assert.assertTrue(addedDocs.remove(doc));
    }

    Assert.assertTrue(addedDocs.isEmpty());
  }

  private final class LevelKey {
    private final ORID rid;
    private final int level;

    private LevelKey(ORID rid, int level) {
      this.rid = rid;
      this.level = level;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LevelKey levelKey = (LevelKey) o;

      if (level != levelKey.level) return false;
      if (!rid.equals(levelKey.rid)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = rid.hashCode();
      result = 31 * result + level;
      return result;
    }
  }
}
