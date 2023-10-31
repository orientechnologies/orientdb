package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class DateBinaryComparatorTest extends BaseMemoryDatabase {

  private final String dateFormat = "yyyy-MM-dd";
  private final String dateValue = "2017-07-18";

  public void beforeTest() {
    super.beforeTest();
    initSchema();
  }

  private void initSchema() {
    OClass testClass = db.getMetadata().getSchema().createClass("Test");
    testClass.createProperty("date", OType.DATE);
    ODocument document = new ODocument(testClass.getName());

    try {
      document.field("date", new SimpleDateFormat(dateFormat).parse(dateValue));
      document.save();
    } catch (ParseException e) {
      e.printStackTrace();
    }
    db.commit();
  }

  @Test
  public void testDateJavaClassPreparedStatement() throws ParseException {
    String str = "SELECT FROM Test WHERE date = :dateParam";
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("dateParam", new SimpleDateFormat(dateFormat).parse(dateValue));

    try (OResultSet result = db.query(str, params)) {
      assertTrue(result.stream().count() == 1);
    }
  }
}
