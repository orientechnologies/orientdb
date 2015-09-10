package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.junit.Assert;
import org.junit.Test;

public class TestIndexFailUpdate {

  @Test
  public void testIndexFailSkipUpdate() {
    OrientGraphFactory factory = new OrientGraphFactory("memory:" + TestIndexFailUpdate.class.getSimpleName());
    OrientGraphNoTx graph = factory.getNoTx();
    ODatabaseDocument db = graph.getRawGraph();
    try {
      OClass V = db.getMetadata().getSchema().getClass("V");
      OClass testMaster = db.getMetadata().getSchema().createClass("test_master", V);
      testMaster.createProperty("name", OType.STRING).setMandatory(true);
      OClass testSlave = db.getMetadata().getSchema().createClass("test_slave", V);
      OProperty prop = testSlave.createProperty("master_link", OType.LINK);
      prop.setMandatory(true);
      prop.setNotNull(true);
      prop.setReadonly(true);

      testSlave.createProperty("slave_number", OType.SHORT).setMandatory(true);

      db.command(new OCommandSQL("CREATE INDEX unique_master_slave ON test_slave (master_link, slave_number) UNIQUE")).execute();

      db.command(new OCommandSQL("CREATE VERTEX test_master SET name='Master 1' ")).execute();
      db.command(new OCommandSQL("CREATE VERTEX test_slave SET master_link=#12:0, slave_number=1 ")).execute();
      OIdentifiable id = db.command(new OCommandSQL("CREATE VERTEX test_slave SET master_link=#12:0, slave_number=2 ")).execute();
      db.command(new OCommandSQL("CREATE VERTEX test_slave SET master_link=#12:0, slave_number=3 ")).execute();
      try {
        db.command(new OCommandSQL("UPDATE " + id.getIdentity() + " SET slave_number=1 ")).execute();
        Assert.fail();
      } catch (OException e) {
      }
      ODocument rec = db.load(id.getIdentity());
      Assert.assertEquals((short)2, rec.field("slave_number"));
    } finally {

      graph.drop();
    }

  }

}
