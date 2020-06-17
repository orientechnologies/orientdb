package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 3/27/14
 */
public class EmbeddedObjectSerializationTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public EmbeddedObjectSerializationTest(@Optional String url) {
    super(url);
  }

  public void testEmbeddedObjectSerialization() {
    final ODocument originalDoc = new ODocument();

    final OCompositeKey compositeKey =
        new OCompositeKey(123, "56", new Date(), new ORecordId("#0:12"));
    originalDoc.field("compositeKey", compositeKey);
    originalDoc.field("int", 12);
    originalDoc.field("val", "test");
    originalDoc.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument loadedDoc = database.load(originalDoc.getIdentity(), "*:-1", true);
    Assert.assertNotSame(loadedDoc, originalDoc);

    final OCompositeKey loadedCompositeKey = loadedDoc.field("compositeKey");
    Assert.assertEquals(loadedCompositeKey, compositeKey);

    originalDoc.delete();
  }

  public void testEmbeddedObjectSerializationInsideOfOtherEmbeddedObjects() {
    final ODocument originalDoc = new ODocument();

    final OCompositeKey compositeKeyOne =
        new OCompositeKey(123, "56", new Date(), new ORecordId("#0:12"));
    final OCompositeKey compositeKeyTwo =
        new OCompositeKey(
            245, "63", new Date(System.currentTimeMillis() + 100), new ORecordId("#0:2"));
    final OCompositeKey compositeKeyThree =
        new OCompositeKey(
            36, "563", new Date(System.currentTimeMillis() + 1000), new ORecordId("#0:23"));

    final ODocument embeddedDocOne = new ODocument();
    embeddedDocOne.field("compositeKey", compositeKeyOne);
    embeddedDocOne.field("val", "test");
    embeddedDocOne.field("int", 10);

    final ODocument embeddedDocTwo = new ODocument();
    embeddedDocTwo.field("compositeKey", compositeKeyTwo);
    embeddedDocTwo.field("val", "test");
    embeddedDocTwo.field("int", 10);

    final ODocument embeddedDocThree = new ODocument();
    embeddedDocThree.field("compositeKey", compositeKeyThree);
    embeddedDocThree.field("val", "test");
    embeddedDocThree.field("int", 10);

    List<ODocument> embeddedCollection = new ArrayList<ODocument>();
    embeddedCollection.add(embeddedDocTwo);
    embeddedCollection.add(embeddedDocThree);

    originalDoc.field("embeddedDoc", embeddedDocOne, OType.EMBEDDED);
    originalDoc.field("embeddedCollection", embeddedCollection, OType.EMBEDDEDLIST);

    originalDoc.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument loadedDocument = database.load(originalDoc.getIdentity(), "*:-1", true);
    Assert.assertNotSame(loadedDocument, originalDoc);

    final ODocument loadedEmbeddedDocOne = loadedDocument.field("embeddedDoc");
    Assert.assertNotSame(loadedEmbeddedDocOne, embeddedDocOne);

    Assert.assertEquals(loadedEmbeddedDocOne.field("compositeKey"), compositeKeyOne);

    List<ODocument> loadedEmbeddedCollection = loadedDocument.field("embeddedCollection");
    Assert.assertNotSame(loadedEmbeddedCollection, embeddedCollection);

    final ODocument loadedEmbeddedDocTwo = loadedEmbeddedCollection.get(0);
    Assert.assertNotSame(loadedEmbeddedDocTwo, embeddedDocTwo);

    Assert.assertEquals(loadedEmbeddedDocTwo.field("compositeKey"), compositeKeyTwo);

    final ODocument loadedEmbeddedDocThree = loadedEmbeddedCollection.get(1);
    Assert.assertNotSame(loadedEmbeddedDocThree, embeddedDocThree);

    Assert.assertEquals(loadedEmbeddedDocThree.field("compositeKey"), compositeKeyThree);

    originalDoc.delete();
  }
}
