package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/21/13
 */
@Test(groups = {"index"})
public class DateIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public DateIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OSchema schema = database.getMetadata().getSchema();

    OClass dateIndexTest = schema.createClass("DateIndexTest");

    dateIndexTest.createProperty("dateField", OType.DATE);
    dateIndexTest.createProperty("dateTimeField", OType.DATETIME);

    dateIndexTest.createProperty("dateList", OType.EMBEDDEDLIST, OType.DATE);
    dateIndexTest.createProperty("dateTimeList", OType.EMBEDDEDLIST, OType.DATETIME);

    dateIndexTest.createProperty("value", OType.STRING);

    dateIndexTest.createIndex("DateIndexTestDateIndex", OClass.INDEX_TYPE.UNIQUE, "dateField");
    dateIndexTest.createIndex(
        "DateIndexTestValueDateIndex", OClass.INDEX_TYPE.UNIQUE, "value", "dateField");

    dateIndexTest.createIndex(
        "DateIndexTestDateTimeIndex", OClass.INDEX_TYPE.UNIQUE, "dateTimeField");
    dateIndexTest.createIndex(
        "DateIndexTestValueDateTimeIndex", OClass.INDEX_TYPE.UNIQUE, "value", "dateTimeField");

    dateIndexTest.createIndex(
        "DateIndexTestValueDateListIndex", OClass.INDEX_TYPE.UNIQUE, "value", "dateList");
    dateIndexTest.createIndex(
        "DateIndexTestValueDateTimeListIndex", OClass.INDEX_TYPE.UNIQUE, "value", "dateTimeList");

    dateIndexTest.createIndex(
        "DateIndexTestDateHashIndex", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "dateField");
    dateIndexTest.createIndex(
        "DateIndexTestValueDateHashIndex",
        OClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value",
        "dateField");

    dateIndexTest.createIndex(
        "DateIndexTestDateTimeHashIndex", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "dateTimeField");
    dateIndexTest.createIndex(
        "DateIndexTestValueDateTimeHashIndex",
        OClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value",
        "dateTimeField");

    dateIndexTest.createIndex(
        "DateIndexTestValueDateListHashIndex",
        OClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value",
        "dateList");
    dateIndexTest.createIndex(
        "DateIndexTestValueDateTimeListHashIndex",
        OClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value",
        "dateTimeList");
  }

  public void testDateIndexes() {
    checkEmbeddedDB();

    final Date dateOne = new Date();

    final Date dateTwo = new Date(dateOne.getTime() + 24 * 60 * 60 * 1000 + 100);

    final ODocument dateDoc = new ODocument("DateIndexTest");

    dateDoc.field("dateField", dateOne);
    dateDoc.field("dateTimeField", dateTwo);

    final List<Date> dateList = new ArrayList<>();

    final Date dateThree = new Date(dateOne.getTime() + 100);
    final Date dateFour = new Date(dateThree.getTime() + 24 * 60 * 60 * 1000 + 100);

    dateList.add(new Date(dateThree.getTime()));
    dateList.add(new Date(dateFour.getTime()));

    final List<Date> dateTimeList = new ArrayList<>();

    dateTimeList.add(new Date(dateThree.getTime()));
    dateTimeList.add(new Date(dateFour.getTime()));

    dateDoc.field("dateList", dateList);
    dateDoc.field("dateTimeList", dateTimeList);

    dateDoc.field("value", "v1");

    dateDoc.save();

    final OIndex dateIndexTestDateIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateIndex");
    try (Stream<ORID> stream = dateIndexTestDateIndex.getInternal().getRids(dateOne)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream = dateIndexTestDateIndex.getInternal().getRids(dateTwo)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestDateTimeIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateTimeIndex");
    try (Stream<ORID> stream = dateIndexTestDateTimeIndex.getInternal().getRids(dateTwo)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream = dateIndexTestDateTimeIndex.getInternal().getRids(dateOne)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateIndex");
    try (Stream<ORID> stream =
        dateIndexTestValueDateIndex.getInternal().getRids(new OCompositeKey("v1", dateOne))) {
      Assert.assertEquals((stream.findAny().orElse(null)), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream =
        dateIndexTestValueDateIndex.getInternal().getRids(new OCompositeKey("v1", dateTwo))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateTimeIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateTimeIndex");
    try (Stream<ORID> stream =
        dateIndexTestValueDateTimeIndex.getInternal().getRids(new OCompositeKey("v1", dateTwo))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream =
        dateIndexTestValueDateTimeIndex.getInternal().getRids(new OCompositeKey("v1", dateOne))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateListIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListIndex");

    try (Stream<ORID> stream =
        dateIndexTestValueDateListIndex.getInternal().getRids(new OCompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream =
        dateIndexTestValueDateListIndex.getInternal().getRids(new OCompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }

    final OIndex dateIndexTestValueDateTimeListIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListIndex");
    try (Stream<ORID> stream =
        dateIndexTestValueDateTimeListIndex
            .getInternal()
            .getRids(new OCompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream =
        dateIndexTestValueDateTimeListIndex
            .getInternal()
            .getRids(new OCompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }

    final OIndex dateIndexTestDateHashIndexIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateHashIndex");
    try (Stream<ORID> stream = dateIndexTestDateHashIndexIndex.getInternal().getRids(dateOne)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream = dateIndexTestDateHashIndexIndex.getInternal().getRids(dateTwo)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestDateTimeHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateTimeHashIndex");
    try (Stream<ORID> stream = dateIndexTestDateTimeHashIndex.getInternal().getRids(dateTwo)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream = dateIndexTestDateTimeHashIndex.getInternal().getRids(dateOne)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateHashIndex");
    try (Stream<ORID> stream =
        dateIndexTestValueDateHashIndex.getInternal().getRids(new OCompositeKey("v1", dateOne))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream =
        dateIndexTestValueDateHashIndex.getInternal().getRids(new OCompositeKey("v1", dateTwo))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateTimeHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateTimeHashIndex");
    try (Stream<ORID> stream =
        dateIndexTestValueDateTimeHashIndex
            .getInternal()
            .getRids(new OCompositeKey("v1", dateTwo))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream =
        dateIndexTestValueDateTimeHashIndex
            .getInternal()
            .getRids(new OCompositeKey("v1", dateOne))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateListHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListHashIndex");

    try (Stream<ORID> stream =
        dateIndexTestValueDateListHashIndex
            .getInternal()
            .getRids(new OCompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream =
        dateIndexTestValueDateListHashIndex
            .getInternal()
            .getRids(new OCompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }

    final OIndex dateIndexTestValueDateTimeListHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListHashIndex");
    try (Stream<ORID> stream =
        dateIndexTestValueDateTimeListHashIndex
            .getInternal()
            .getRids(new OCompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<ORID> stream =
        dateIndexTestValueDateTimeListHashIndex
            .getInternal()
            .getRids(new OCompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
  }
}
