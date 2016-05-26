package com.orientechnologies.orient.core.serialization.serializer.record.string;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseImportTest;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

import static com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSONTest.TestCustom.ONE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Created by tglman on 25/05/16.
 */
public class ORecordSerializerJSONTest {

  public enum TestCustom {
    ONE, TWO
  }

  @Test
  public void testCustomSerialization() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ODatabaseImportTest.class.getSimpleName());
    db.create();
    try {
      OClass klass = db.getMetadata().getSchema().createClass("TestCustom");
      klass.createProperty("test", OType.CUSTOM);
      ODocument doc = new ODocument("TestCustom");
      doc.field("test", ONE, OType.CUSTOM);

      String json = doc.toJSON();

      ODocument doc1 = new ODocument();
      doc1.fromJSON(json);
      assertEquals(TestCustom.valueOf((String) doc1.field("test")), TestCustom.ONE);
    } finally {
      db.drop();
    }
  }

}
