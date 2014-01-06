package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.*;

@Test
public class ORidBagAtomicUpdateTest {
  private ODatabaseDocumentTx db;
  private int                 topThreshold;
  private int                 bottomThreshold;

  @BeforeClass
  public void setUp() throws Exception {
    final String buildDirectory = System.getProperty("buildDirectory", ".");

    db = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/testdb/ORidBagAtomicUpdateTest");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();
    db.declareIntent(new OIntentMassiveInsert());
  }

  @AfterClass
  public void tearDown() throws Exception {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    db.drop();
  }

  @BeforeMethod
  public void beforeMethod() {
    topThreshold = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold = OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
  }

  @AfterMethod
  public void afterMethod() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }

  public void testAddTwoNewDocuments() {
    db.begin();
    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    rootDoc.save();
    db.commit();

    db.begin();

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save();

    db.rollback();

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 0);
  }

  public void testAddTwoNewDocumentsWithCME() {
    final ODocument cmeDoc = new ODocument();
    cmeDoc.save();

    db.begin();
    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    rootDoc.save();
    db.commit();

    ODocument staleCMEDoc = db.load(cmeDoc.getIdentity());
    Assert.assertNotSame(staleCMEDoc, cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save();

    db.begin();

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    staleCMEDoc.field("v", "v1");
    staleCMEDoc.save();

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 0);
  }

  public void testAddTwoAdditionalNewDocuments() {
    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save();

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.begin();

    ODocument docThree = new ODocument();
    ODocument docFour = new ODocument();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save();

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

  public void testAddTwoAdditionalNewDocumentsWithCME() {
    final ODocument cmeDoc = new ODocument();
    cmeDoc.save();

    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save();

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    ODocument staleCMEDoc = db.load(cmeDoc.getIdentity());
    Assert.assertNotSame(staleCMEDoc, cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save();

    db.begin();

    ODocument docThree = new ODocument();
    ODocument docFour = new ODocument();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save();

    staleCMEDoc.field("v", "v1");
    staleCMEDoc.save();

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

  public void testAddTwoSavedDocuments() {
    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    docOne.save();
    ODocument docTwo = new ODocument();
    docTwo.save();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save();

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
  }

  public void testAddTwoAdditionalSavedDocuments() {
    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save();

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.begin();

    ODocument docThree = new ODocument();
    docThree.save();
    ODocument docFour = new ODocument();
    docFour.save();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save();

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

  public void testAddTwoAdditionalSavedDocumentsWithCME() {
    final ODocument cmeDoc = new ODocument();
    cmeDoc.save();

    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save();

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    ODocument staleCMEDoc = db.load(cmeDoc.getIdentity());
    Assert.assertNotSame(staleCMEDoc, cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save();

    db.begin();

    ODocument docThree = new ODocument();
    docThree.save();
    ODocument docFour = new ODocument();
    docFour.save();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save();

    staleCMEDoc.field("v", "vn");
    staleCMEDoc.save();

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

  public void testAddInternalDocumentsAndSubDocuments() {
    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    docOne.save();

    ODocument docTwo = new ODocument();
    docTwo.save();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save();

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());
    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.begin();

    ODocument docThree = new ODocument();
    docThree.save();

    ODocument docFour = new ODocument();
    docFour.save();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save();

    ODocument docThreeOne = new ODocument();
    docThreeOne.save();

    ODocument docThreeTwo = new ODocument();
    docThreeTwo.save();

    ORidBag ridBagThree = new ORidBag();
    ridBagThree.add(docThreeOne);
    ridBagThree.add(docThreeTwo);
    docThree.field("ridBag", ridBagThree);

    docThree.save();

    ODocument docFourOne = new ODocument();
    docFourOne.save();

    ODocument docFourTwo = new ODocument();
    docFourTwo.save();

    ORidBag ridBagFour = new ORidBag();
    ridBagFour.add(docFourOne);
    ridBagFour.add(docFourTwo);

    docFour.field("ridBag", ridBagFour);

    docFour.save();

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(Arrays.asList(docOne, docTwo));

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  public void testAddInternalDocumentsAndSubDocumentsWithCME() {
    final ODocument cmeDoc = new ODocument();
    cmeDoc.save();

    db.begin();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    docOne.save();

    ODocument docTwo = new ODocument();
    docTwo.save();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save();

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());
    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    ODocument staleCMEDoc = db.load(cmeDoc.getIdentity());
    Assert.assertNotSame(staleCMEDoc, cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save();

    db.begin();

    ODocument docThree = new ODocument();
    docThree.save();

    ODocument docFour = new ODocument();
    docFour.save();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save();

    ODocument docThreeOne = new ODocument();
    docThreeOne.save();

    ODocument docThreeTwo = new ODocument();
    docThreeTwo.save();

    ORidBag ridBagThree = new ORidBag();
    ridBagThree.add(docThreeOne);
    ridBagThree.add(docThreeTwo);
    docThree.field("ridBag", ridBagThree);

    docThree.save();

    ODocument docFourOne = new ODocument();
    docFourOne.save();

    ODocument docFourTwo = new ODocument();
    docFourTwo.save();

    ORidBag ridBagFour = new ORidBag();
    ridBagFour.add(docFourOne);
    ridBagFour.add(docFourTwo);

    docFour.field("ridBag", ridBagFour);

    docFour.save();

    staleCMEDoc.field("v", "vn");
    staleCMEDoc.save();

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

  public void testAddTwoSavedDocumentsWithoutTx() {
    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    rootDoc.save();

    ODocument staleRooDoc = db.load(rootDoc.getIdentity());
    ORidBag staleRidBag = staleRooDoc.field("ridBag");

    staleRidBag.add(docOne);
    staleRidBag.add(docTwo);

    rootDoc.setDirty();
    rootDoc.save();
    try {
      staleRooDoc.save();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 0);
  }

  public void testAddOneSavedDocumentsAndDeleteOneWithoutTx() {
    ODocument docOne = new ODocument();
    docOne.save();

    ODocument docTwo = new ODocument();
    docTwo.save();

    ODocument docThree = new ODocument();
    docThree.save();

    ODocument rootDoc = new ODocument();

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save();

    ODocument staleRooDoc = db.load(rootDoc.getIdentity());
    ORidBag staleRidBag = staleRooDoc.field("ridBag");

    Iterator<OIdentifiable> iterator = staleRidBag.iterator();
    iterator.next();
    iterator.remove();

    staleRidBag.add(docThree);

    rootDoc.setDirty();
    rootDoc.save();
    try {
      staleRooDoc.save();
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
        docToAdd.save();
        addedDocuments.add(docToAdd.getIdentity());
        ridBag.add(docToAdd);
      }

      document.save();

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

        if (!iterator.hasNext())
          iterator = staleRidBag.iterator();
      }

      for (k = 0; k < amountOfAddedDocsAfterSave; k++) {
        ODocument docToAdd = new ODocument();
        docToAdd.save();

        staleRidBag.add(docToAdd);
      }

      Assert.assertEquals(addedDocuments.size() - amountOfDeletedDocs + amountOfAddedDocsAfterSave, staleRidBag.size());

      document.setDirty();
      document.save();
      try {
        staleDocument.save();
        Assert.fail();
      } catch (OConcurrentModificationException e) {
      }

      document = db.load(document.getIdentity());
      ridBag = document.field("ridBag");

      Assert.assertEquals(ridBag.size(), addedDocuments.size());
      iterator = ridBag.iterator();
      while (iterator.hasNext())
        Assert.assertTrue(addedDocuments.remove(iterator.next()));

      Assert.assertEquals(addedDocuments.size(), 0);
    }
  }

  public void testVisibility() {
    ODocument document = new ODocument();
    ORidBag ridBag = new ORidBag();

    document.field("ridBag", ridBag);
    ODocument docOne = new ODocument();
    docOne.save();

    ODocument docTwo = new ODocument();
    docTwo.save();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.save();

    ODocument copyOne = db.load(document.getIdentity());
    ODocument copyTwo = db.load(document.getIdentity());

    Assert.assertNotSame(copyOne, copyTwo);

    ORidBag ridBagOne = copyOne.field("ridBag");
    ORidBag ridBagTwo = copyTwo.field("ridBag");

    ODocument docTree = new ODocument();
    docTree.save();

    ODocument docFour = new ODocument();
    docFour.save();

    ridBagOne.remove(docOne);

    ridBagOne.add(docTree);
    ridBagTwo.add(docFour);

    Assert.assertEquals(ridBagOne.size(), 2);
    Assert.assertEquals(ridBagTwo.size(), 3);
  }

  public void testRandomChangedInTx() {
    Random rnd = new Random();

    final int levels = rnd.nextInt(2) + 1;
    final List<Integer> amountOfAddedDocsPerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfAddedDocsAfterSavePerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfDeletedDocsPerLevel = new ArrayList<Integer>();
    Map<LevelKey, List<OIdentifiable>> addedDocPerLevel = new HashMap<LevelKey, List<OIdentifiable>>();

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

  public void testRandomChangedInTxWithCME() {
    final ODocument cmeDoc = new ODocument();
    cmeDoc.save();

    Random rnd = new Random();

    final int levels = rnd.nextInt(2) + 1;
    final List<Integer> amountOfAddedDocsPerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfAddedDocsAfterSavePerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfDeletedDocsPerLevel = new ArrayList<Integer>();
    Map<LevelKey, List<OIdentifiable>> addedDocPerLevel = new HashMap<LevelKey, List<OIdentifiable>>();

    for (int i = 0; i < levels; i++) {
      amountOfAddedDocsPerLevel.add(rnd.nextInt(5) + 10);
      amountOfAddedDocsAfterSavePerLevel.add(rnd.nextInt(5) + 5);
      amountOfDeletedDocsPerLevel.add(rnd.nextInt(5) + 5);
    }

    ODocument staleCMEDoc = db.load(cmeDoc.getIdentity());
    Assert.assertNotSame(staleCMEDoc, cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save();

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
    staleCMEDoc.save();

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    assertDocsAfterRollback(0, levels, addedDocPerLevel, rootDoc);
  }

  public void testFromEmbeddedToSBTreeRollback() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save();

    db.begin();

    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save();

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    db.begin();
    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    document.save();
    db.rollback();

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (OIdentifiable identifiable : ridBag)
      Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  public void testFromEmbeddedToSBTreeTXWithCME() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    ODocument cmeDocument = new ODocument();
    cmeDocument.field("v", "v1");
    cmeDocument.save();

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save();

    db.begin();

    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save();

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    ODocument staleDocument = db.load(cmeDocument.getIdentity());
    Assert.assertNotSame(staleDocument, cmeDocument);

    cmeDocument.field("v", "v234");
    cmeDocument.save();

    db.begin();
    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    document.save();
    staleDocument.field("v", "ver");
    staleDocument.save();

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (OIdentifiable identifiable : ridBag)
      Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  public void testFromEmbeddedToSBTreeWithCME() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save();

    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save();

    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    ODocument cmeDocument = db.load(document.getIdentity());
    Assert.assertNotSame(cmeDocument, document);
    cmeDocument.field("v", "v1");
    cmeDocument.save();

    for (int i = 0; i < 3; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    try {
      document.save();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (OIdentifiable identifiable : ridBag)
      Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  public void testFromSBTreeToEmbeddedRollback() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save();

    db.begin();

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save();

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

    document.save();
    db.rollback();

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(!ridBag.isEmbedded());

    for (OIdentifiable identifiable : ridBag)
      Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  public void testFromSBTreeToEmbeddedTxWithCME() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    ODocument cmeDoc = new ODocument();
    cmeDoc.save();

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save();

    db.begin();

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save();

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 10);
    Assert.assertTrue(!ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    ODocument staleDoc = db.load(cmeDoc.getIdentity());
    Assert.assertNotSame(staleDoc, cmeDoc);

    cmeDoc.field("v", "sd");
    cmeDoc.save();

    db.begin();
    for (int i = 0; i < 4; i++) {
      OIdentifiable docToRemove = docsToAdd.get(i);
      ridBag.remove(docToRemove);
    }

    Assert.assertTrue(document.isDirty());

    document.save();

    staleDoc.field("v", "d");
    staleDoc.save();

    try {
      db.commit();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(!ridBag.isEmbedded());

    for (OIdentifiable identifiable : ridBag)
      Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  public void testFromSBTreeToEmbeddedWithCME() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    List<OIdentifiable> docsToAdd = new ArrayList<OIdentifiable>();

    ODocument document = new ODocument();

    ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag);
    document.save();

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save();

    Assert.assertEquals(docsToAdd.size(), 10);
    Assert.assertTrue(!ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    ODocument cmeDoc = db.load(document.getIdentity());
    cmeDoc.field("v", "v1");
    cmeDoc.save();

    for (int i = 0; i < 4; i++) {
      OIdentifiable docToRemove = docsToAdd.get(i);
      ridBag.remove(docToRemove);
    }

    Assert.assertTrue(document.isDirty());

    try {
      document.save();
      Assert.fail();
    } catch (OConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(!ridBag.isEmbedded());

    for (OIdentifiable identifiable : ridBag)
      Assert.assertTrue(docsToAdd.remove(identifiable));

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  private void createDocsForLevel(final List<Integer> amountOfAddedDocsPerLevel, int level, int levels,
      Map<LevelKey, List<OIdentifiable>> addedDocPerLevel, ODocument rootDoc) {

    int docs = amountOfAddedDocsPerLevel.get(level);

    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>();
    addedDocPerLevel.put(new LevelKey(rootDoc.getIdentity(), level), addedDocs);

    ORidBag ridBag = new ORidBag();
    rootDoc.field("ridBag", ridBag);

    for (int i = 0; i < docs; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();

      addedDocs.add(docToAdd.getIdentity());
      ridBag.add(docToAdd);

      if (level + 1 < levels)
        createDocsForLevel(amountOfAddedDocsPerLevel, level + 1, levels, addedDocPerLevel, docToAdd);
    }

    rootDoc.save();
  }

  private void deleteDocsForLevel(List<Integer> amountOfDeletedDocsPerLevel, int level, int levels, ODocument rootDoc, Random rnd) {
    ORidBag ridBag = rootDoc.field("ridBag");
    for (OIdentifiable identifiable : ridBag) {
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

      if (!iterator.hasNext())
        iterator = ridBag.iterator();
    }
    rootDoc.save();

  }

  private void addDocsForLevel(List<Integer> amountOfAddedDocsAfterSavePerLevel, int level, int levels, ODocument rootDoc) {
    ORidBag ridBag = rootDoc.field("ridBag");

    for (OIdentifiable identifiable : ridBag) {
      ODocument doc = identifiable.getRecord();
      if (level + 1 < levels)
        addDocsForLevel(amountOfAddedDocsAfterSavePerLevel, level + 1, levels, doc);
    }

    int docs = amountOfAddedDocsAfterSavePerLevel.get(level);
    for (int i = 0; i < docs; i++) {
      ODocument docToAdd = new ODocument();
      docToAdd.save();

      ridBag.add(docToAdd);
    }
    rootDoc.save();

  }

  private void assertDocsAfterRollback(int level, int levels, Map<LevelKey, List<OIdentifiable>> addedDocPerLevel, ODocument rootDoc) {
    ORidBag ridBag = rootDoc.field("ridBag");
    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>(addedDocPerLevel.get(new LevelKey(rootDoc.getIdentity(), level)));

    Iterator<OIdentifiable> iterator = ridBag.iterator();
    while (iterator.hasNext()) {
      ODocument doc = iterator.next().getRecord();
      if (level + 1 < levels)
        assertDocsAfterRollback(level + 1, levels, addedDocPerLevel, doc);
      else
        Assert.assertNull(doc.field("ridBag"));

      Assert.assertTrue(addedDocs.remove(doc));
    }

    Assert.assertTrue(addedDocs.isEmpty());
  }

  private final class LevelKey {
    private final ORID rid;
    private final int  level;

    private LevelKey(ORID rid, int level) {
      this.rid = rid;
      this.level = level;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      LevelKey levelKey = (LevelKey) o;

      if (level != levelKey.level)
        return false;
      if (!rid.equals(levelKey.rid))
        return false;

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
