package com.orientechnologies.orient.test.database.auto;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/21/13
 */
@Test(groups = { "index" })
public class DateIndexTest {
  private ODatabaseDocumentTx database;

  @Parameters(value = "url")
  public DateIndexTest(String url) {
    database = new ODatabaseDocumentTx(url);
  }

  @BeforeClass
  public void beforeClass() {
    database.open("admin", "admin");

    final OSchema schema = database.getMetadata().getSchema();

    OClass dateIndexTest = schema.createClass("DateIndexTest");

    dateIndexTest.createProperty("dateField", OType.DATE);
    dateIndexTest.createProperty("dateTimeField", OType.DATETIME);

    dateIndexTest.createProperty("dateList", OType.EMBEDDEDLIST, OType.DATE);
    dateIndexTest.createProperty("dateTimeList", OType.EMBEDDEDLIST, OType.DATETIME);

    dateIndexTest.createProperty("value", OType.STRING);

    dateIndexTest.createIndex("DateIndexTestDateIndex", OClass.INDEX_TYPE.UNIQUE, "dateField");
    dateIndexTest.createIndex("DateIndexTestValueDateIndex", OClass.INDEX_TYPE.UNIQUE, "value", "dateField");

    dateIndexTest.createIndex("DateIndexTestDateTimeIndex", OClass.INDEX_TYPE.UNIQUE, "dateTimeField");
    dateIndexTest.createIndex("DateIndexTestValueDateTimeIndex", OClass.INDEX_TYPE.UNIQUE, "value", "dateTimeField");

    dateIndexTest.createIndex("DateIndexTestValueDateListIndex", OClass.INDEX_TYPE.UNIQUE, "value", "dateList");
    dateIndexTest.createIndex("DateIndexTestValueDateTimeListIndex", OClass.INDEX_TYPE.UNIQUE, "value", "dateTimeList");

    dateIndexTest.createIndex("DateIndexTestDateHashIndex", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "dateField");
    dateIndexTest.createIndex("DateIndexTestValueDateHashIndex", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "value", "dateField");

    dateIndexTest.createIndex("DateIndexTestDateTimeHashIndex", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "dateTimeField");
    dateIndexTest.createIndex("DateIndexTestValueDateTimeHashIndex", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "value", "dateTimeField");

    dateIndexTest.createIndex("DateIndexTestValueDateListHashIndex", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "value", "dateList");
    dateIndexTest.createIndex("DateIndexTestValueDateTimeListHashIndex", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "value",
        "dateTimeList");

    schema.save();

    database.close();
  }

  @AfterMethod
  public void afterMethod() {
    database.close();
  }

  public void testDateIndexes() {
    database.open("admin", "admin");

    final Date dateOne = new Date();

    final Date dateTwo = new Date(dateOne.getTime() + 24 * 60 * 60 * 1000 + 100);

    final ODocument dateDoc = new ODocument("DateIndexTest");

    dateDoc.field("dateField", dateOne);
    dateDoc.field("dateTimeField", dateTwo);

    final List<Date> dateList = new ArrayList<Date>();

    final Date dateThree = new Date(dateOne.getTime() + 100);
    final Date dateFour = new Date(dateThree.getTime() + 24 * 60 * 60 * 1000 + 100);

    dateList.add(new Date(dateThree.getTime()));
    dateList.add(new Date(dateFour.getTime()));

    final List<Date> dateTimeList = new ArrayList<Date>();

    dateTimeList.add(new Date(dateThree.getTime()));
    dateTimeList.add(new Date(dateFour.getTime()));

    dateDoc.field("dateList", dateList);
    dateDoc.field("dateTimeList", dateTimeList);

    dateDoc.field("value", "v1");

    dateDoc.save();

    final OIndex dateIndexTestDateIndex = database.getMetadata().getIndexManager().getIndex("DateIndexTestDateIndex");
    Assert.assertEquals(((OIdentifiable) dateIndexTestDateIndex.get(dateOne)).getIdentity(), dateDoc.getIdentity());
    Assert.assertNull(dateIndexTestDateIndex.get(dateTwo));

    final OIndex dateIndexTestDateTimeIndex = database.getMetadata().getIndexManager().getIndex("DateIndexTestDateTimeIndex");
    Assert.assertEquals(((OIdentifiable) dateIndexTestDateTimeIndex.get(dateTwo)).getIdentity(), dateDoc.getIdentity());
    Assert.assertNull(dateIndexTestDateTimeIndex.get(dateOne));

    final OIndex dateIndexTestValueDateIndex = database.getMetadata().getIndexManager().getIndex("DateIndexTestValueDateIndex");
    Assert.assertEquals(((OIdentifiable) dateIndexTestValueDateIndex.get(new OCompositeKey("v1", dateOne))).getIdentity(),
        dateDoc.getIdentity());
    Assert.assertNull(dateIndexTestValueDateIndex.get(new OCompositeKey("v1", dateTwo)));

    final OIndex dateIndexTestValueDateTimeIndex = database.getMetadata().getIndexManager()
        .getIndex("DateIndexTestValueDateTimeIndex");
    Assert.assertEquals(((OIdentifiable) dateIndexTestValueDateTimeIndex.get(new OCompositeKey("v1", dateTwo))).getIdentity(),
        dateDoc.getIdentity());
    Assert.assertNull(dateIndexTestValueDateTimeIndex.get(new OCompositeKey("v1", dateOne)));

    final OIndex dateIndexTestValueDateListIndex = database.getMetadata().getIndexManager()
        .getIndex("DateIndexTestValueDateListIndex");

    Assert.assertEquals(((OIdentifiable) dateIndexTestValueDateListIndex.get(new OCompositeKey("v1", dateThree))).getIdentity(),
        dateDoc.getIdentity());
    Assert.assertEquals(((OIdentifiable) dateIndexTestValueDateListIndex.get(new OCompositeKey("v1", dateFour))).getIdentity(),
        dateDoc.getIdentity());

    final OIndex dateIndexTestValueDateTimeListIndex = database.getMetadata().getIndexManager()
        .getIndex("DateIndexTestValueDateListIndex");
    Assert.assertEquals(
        ((OIdentifiable) dateIndexTestValueDateTimeListIndex.get(new OCompositeKey("v1", dateThree))).getIdentity(),
        dateDoc.getIdentity());
    Assert.assertEquals(((OIdentifiable) dateIndexTestValueDateTimeListIndex.get(new OCompositeKey("v1", dateFour))).getIdentity(),
        dateDoc.getIdentity());

    final OIndex dateIndexTestDateHashIndexIndex = database.getMetadata().getIndexManager().getIndex("DateIndexTestDateHashIndex");
    Assert.assertEquals(((OIdentifiable) dateIndexTestDateHashIndexIndex.get(dateOne)).getIdentity(), dateDoc.getIdentity());
    Assert.assertNull(dateIndexTestDateHashIndexIndex.get(dateTwo));

    final OIndex dateIndexTestDateTimeHashIndex = database.getMetadata().getIndexManager()
        .getIndex("DateIndexTestDateTimeHashIndex");
    Assert.assertEquals(((OIdentifiable) dateIndexTestDateTimeHashIndex.get(dateTwo)).getIdentity(), dateDoc.getIdentity());
    Assert.assertNull(dateIndexTestDateTimeHashIndex.get(dateOne));

    final OIndex dateIndexTestValueDateHashIndex = database.getMetadata().getIndexManager()
        .getIndex("DateIndexTestValueDateHashIndex");
    Assert.assertEquals(((OIdentifiable) dateIndexTestValueDateHashIndex.get(new OCompositeKey("v1", dateOne))).getIdentity(),
        dateDoc.getIdentity());
    Assert.assertNull(dateIndexTestValueDateHashIndex.get(new OCompositeKey("v1", dateTwo)));

    final OIndex dateIndexTestValueDateTimeHashIndex = database.getMetadata().getIndexManager()
        .getIndex("DateIndexTestValueDateTimeHashIndex");
    Assert.assertEquals(((OIdentifiable) dateIndexTestValueDateTimeHashIndex.get(new OCompositeKey("v1", dateTwo))).getIdentity(),
        dateDoc.getIdentity());
    Assert.assertNull(dateIndexTestValueDateTimeHashIndex.get(new OCompositeKey("v1", dateOne)));

    final OIndex dateIndexTestValueDateListHashIndex = database.getMetadata().getIndexManager()
        .getIndex("DateIndexTestValueDateListHashIndex");

    Assert.assertEquals(
        ((OIdentifiable) dateIndexTestValueDateListHashIndex.get(new OCompositeKey("v1", dateThree))).getIdentity(),
        dateDoc.getIdentity());
    Assert.assertEquals(((OIdentifiable) dateIndexTestValueDateListHashIndex.get(new OCompositeKey("v1", dateFour))).getIdentity(),
        dateDoc.getIdentity());

    final OIndex dateIndexTestValueDateTimeListHashIndex = database.getMetadata().getIndexManager()
        .getIndex("DateIndexTestValueDateListHashIndex");
    Assert.assertEquals(
        ((OIdentifiable) dateIndexTestValueDateTimeListHashIndex.get(new OCompositeKey("v1", dateThree))).getIdentity(),
        dateDoc.getIdentity());
    Assert.assertEquals(
        ((OIdentifiable) dateIndexTestValueDateTimeListHashIndex.get(new OCompositeKey("v1", dateFour))).getIdentity(),
        dateDoc.getIdentity());
  }
}
