package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

public class DefaultClusterTest {
  @Test
  public void defaultClusterTest() {
    OrientDB context = new OrientDB("embedded:", OrientDBConfig.defaultConfig());

    context.create("test", ODatabaseType.MEMORY);

    try (ODatabaseSession db = context.open("test", "admin", "admin")) {

      OVertex v = db.newVertex("V");

      v.setProperty("embedded", new ODocument());

      db.save(v);

      ODocument embedded = v.getProperty("embedded");
      Assert.assertFalse("Found: " + embedded.getIdentity(), embedded.getIdentity().isValid());
    }
  }
}
