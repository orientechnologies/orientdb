package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

public class DefaultClusterTest {
  @Test
  public void defaultClusterTest() {
    final OrientDB context =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    try (final ODatabaseSession db =
        context.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
      final OVertex vertex = db.newVertex("V");
      vertex.setProperty("embedded", new ODocument());
      db.save(vertex);

      final ODocument embedded = vertex.getProperty("embedded");
      Assert.assertFalse("Found: " + embedded.getIdentity(), embedded.getIdentity().isValid());
    }
  }
}
