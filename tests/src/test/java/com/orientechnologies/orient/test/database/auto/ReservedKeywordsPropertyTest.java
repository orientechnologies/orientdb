package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/5/2015
 */
public class ReservedKeywordsPropertyTest extends DocumentDBBaseTest {
  private final static String CLASS_NAME = "testClass";

  @Parameters(value = "url")
  public ReservedKeywordsPropertyTest(@Optional String url) {
    super(url);
  }

  @Test
  public void trivialTest() {
    OSchema schema = database.getMetadata().getSchema();
    if (schema.existsClass(CLASS_NAME)) {
      schema.dropClass(CLASS_NAME);
    }

    OClass clz = schema.createClass(CLASS_NAME);
    for (String keyword : OClassImpl.RESERVED_KEYWORDS) {
      try {
        clz.createProperty(keyword, OType.LONG);
      } catch (OSchemaException ex) {
        // Good
      } catch (Throwable throwable) {
        throwable.printStackTrace();
        Assert.assertTrue(false);
      }
    }
  }
}