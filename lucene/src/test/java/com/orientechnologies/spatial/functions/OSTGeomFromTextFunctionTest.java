package com.orientechnologies.spatial.functions;

import java.util.Arrays;
import java.util.List;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.spatial.BaseSpatialLuceneTest;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class OSTGeomFromTextFunctionTest extends BaseSpatialLuceneTest {

  protected static final WKTReader wktReader = new WKTReader();

  @Test
  public void test() {
    boolean prevValue = OGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.getValueAsBoolean();
    OGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.setValue(true);
    try {
      OSTGeomFromTextFunction func = new OSTGeomFromTextFunction();
      ODocument item = (ODocument) func.execute(null, null, null, new Object[]{"POINT (100.0 80.0)"}, null);
      Assert.assertEquals(2, ((List) item.getProperty("coordinates")).size());

      item = (ODocument) func.execute(null, null, null, new Object[]{"POINT Z(100.0 80.0 10)"}, null);
      Assert.assertEquals(3, ((List) item.getProperty("coordinates")).size());

    } finally {
      OGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.setValue(prevValue);
    }
  }

}
