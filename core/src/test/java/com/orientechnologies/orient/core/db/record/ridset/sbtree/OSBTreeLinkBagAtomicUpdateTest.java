package com.orientechnologies.orient.core.db.record.ridset.sbtree;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;

@Test
public class OSBTreeLinkBagAtomicUpdateTest {
  private ODatabaseDocumentTx db;

  @BeforeClass
  public void setUp() throws Exception {
    db = new ODatabaseDocumentTx("plocal:target/testdb/OSBTreeLinkBagAtomicUpdateTest");
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

  public void testAddTwoNewDocuments() {
    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();

    ODocument rootDoc = new ODocument();

    OSBTreeRidBag ridBag = new OSBTreeRidBag();
    rootDoc.field("ridBag", ridBag);

    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save();

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
  }

  public void testAddTwoAdditionalNewDocuments() {
    db.begin();

    ODocument rootDoc = new ODocument();

    OSBTreeRidBag ridBag = new OSBTreeRidBag();
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

  public void testAddTwoSavedDocuments() {
    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();

    ODocument rootDoc = new ODocument();

    OSBTreeRidBag ridBag = new OSBTreeRidBag();
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

    OSBTreeRidBag ridBag = new OSBTreeRidBag();
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

  public void testAddInternalDocumentsAndSubDocuments() {
    db.begin();

    ODocument rootDoc = new ODocument();

    OSBTreeRidBag ridBag = new OSBTreeRidBag();
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

    OSBTreeRidBag ridBagThree = new OSBTreeRidBag();
    ridBagThree.add(docThreeOne);
    ridBagThree.add(docThreeTwo);
    docThree.field("ridBag", ridBagThree);

    docThree.save();

    ODocument docFourOne = new ODocument();
    docFourOne.save();

    ODocument docFourTwo = new ODocument();
    docFourTwo.save();

    OSBTreeRidBag ridBagFour = new OSBTreeRidBag();
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

  public void testAddTwoSavedDocumentsWithoutTx() {
    ODocument docOne = new ODocument();
    ODocument docTwo = new ODocument();

    ODocument rootDoc = new ODocument();

    OSBTreeRidBag ridBag = new OSBTreeRidBag();
    rootDoc.field("ridBag", ridBag);

    rootDoc.save();

    ODocument staleRooDoc = db.load(rootDoc.getIdentity());
    OSBTreeRidBag staleRidBag = staleRooDoc.field("ridBag");

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

    OSBTreeRidBag ridBag = new OSBTreeRidBag();
    rootDoc.field("ridBag", ridBag);

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save();

    ODocument staleRooDoc = db.load(rootDoc.getIdentity());
    OSBTreeRidBag staleRidBag = staleRooDoc.field("ridBag");

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
    final Random rnd = new Random();
    final int iterations = 20;
    for (int n = 0; n < iterations; n++) {
      final int amountOfAddedDocs = rnd.nextInt(10) + 10;
      final int amountOfAddedDocsAfterSave = rnd.nextInt(5) + 5;
      final int amountOfDeletedDocs = rnd.nextInt(5) + 5;

      List<OIdentifiable> addedDocuments = new ArrayList<OIdentifiable>();

      ODocument document = new ODocument();
      OSBTreeRidBag ridBag = new OSBTreeRidBag();
      document.field("ridBag", ridBag);

      for (int i = 0; i < amountOfAddedDocs; i++) {
        final ODocument docToAdd = new ODocument();
        docToAdd.save();
        addedDocuments.add(docToAdd.getIdentity());
        ridBag.add(docToAdd);
      }

      document.save();

      ODocument staleDocument = db.load(document.getIdentity());
      OSBTreeRidBag staleRidBag = staleDocument.field("ridBag");

      Assert.assertNotSame(document, staleDocument);
      Assert.assertNotSame(ridBag, staleRidBag);

      int k = 0;
      Iterator<OIdentifiable> iterator = staleRidBag.iterator();
      while (k < amountOfDeletedDocs) {
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
    OSBTreeRidBag ridBag = new OSBTreeRidBag();

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

    OSBTreeRidBag ridBagOne = copyOne.field("ridBag");
    OSBTreeRidBag ridBagTwo = copyTwo.field("ridBag");

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

  private void createDocsForLevel(final List<Integer> amountOfAddedDocsPerLevel, int level, int levels,
      Map<LevelKey, List<OIdentifiable>> addedDocPerLevel, ODocument rootDoc) {

    int docs = amountOfAddedDocsPerLevel.get(level);

    List<OIdentifiable> addedDocs = new ArrayList<OIdentifiable>();
    addedDocPerLevel.put(new LevelKey(rootDoc.getIdentity(), level), addedDocs);

    OSBTreeRidBag ridBag = new OSBTreeRidBag();
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
    OSBTreeRidBag ridBag = rootDoc.field("ridBag");
    for (OIdentifiable identifiable : ridBag) {
      ODocument doc = identifiable.getRecord();
      if (level + 1 < levels)
        deleteDocsForLevel(amountOfDeletedDocsPerLevel, level + 1, levels, doc, rnd);
    }

    int docs = amountOfDeletedDocsPerLevel.get(level);

    int k = 0;
    Iterator<OIdentifiable> iterator = ridBag.iterator();
    while (k < docs) {
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
    OSBTreeRidBag ridBag = rootDoc.field("ridBag");

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
    OSBTreeRidBag ridBag = rootDoc.field("ridBag");
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
