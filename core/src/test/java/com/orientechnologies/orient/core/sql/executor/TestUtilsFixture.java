package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import org.apache.commons.lang.RandomStringUtils;

/** Created by olena.kolesnyk on 28/07/2017. */
public class TestUtilsFixture extends BaseMemoryDatabase {

  protected OClass createClassInstance() {
    return getDBSchema().createClass(generateClassName());
  }

  protected OClass createChildClassInstance(OClass superclass) {
    return getDBSchema().createClass(generateClassName(), superclass);
  }

  private OSchema getDBSchema() {
    return db.getMetadata().getSchema();
  }

  private static String generateClassName() {
    return "Class" + RandomStringUtils.randomNumeric(10);
  }
}
