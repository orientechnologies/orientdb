package com.orientechnologies.spatial.functions;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.spatial.BaseSpatialLuceneTest;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.io.WKTReader;

public class OSTAsTextFunctionTest extends BaseSpatialLuceneTest {

  protected static final WKTReader wktReader = new WKTReader();

  @Test
  public void test() {
    boolean prevValue = OGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.getValueAsBoolean();
    OGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.setValue(true);

    String[] values = {
      "POINT (100.1 80.2)",
      "POINT Z (100.1 80.2 0.3)",
      "LINESTRING (1 1, 1 2, 1 3, 2 2)",
      "LINESTRING Z (1 1 0, 1 2 0, 1 3 1, 2 2 0)"
    };
    try {
      OSTGeomFromTextFunction func = new OSTGeomFromTextFunction();
      OSTAsTextFunction func2 = new OSTAsTextFunction();

      for (String value : values) {
        ODocument item = (ODocument) func.execute(null, null, null, new Object[] {value}, null);

        String result = (String) func2.execute(null, null, null, new Object[] {item}, null);

        Assert.assertEquals(value, result);
      }

    } finally {
      OGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.setValue(prevValue);
    }
  }
}
