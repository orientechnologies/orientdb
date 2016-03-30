package com.orientechnologies.orient.console;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 14/03/16.
 */
public class OConsoleDatatabaseAppTest {

  @Test
  public void testSelectBinaryDoc() throws IOException {
    final StringBuilder builder = new StringBuilder();
    OConsoleDatabaseApp app = new OConsoleDatabaseApp(new String[] {}) {
      @Override
      public void message(String iMessage, Object... iArgs) {
        builder.append(String.format(iMessage,iArgs)).append("\n");
      }
    };

    app.createDatabase("memory:test", null, null, "memory", null);
    ODatabaseDocument db = app.getCurrentDatabase();
    db.addCluster("blobTest");
    ORecord record = db.save(new ORecordBytes("blobContent".getBytes()), "blobTest");
    builder.setLength(0);
    app.select(" from " + record.getIdentity() +" limit -1 ");
    assertTrue(builder.toString().contains("<binary>"));

  }

}
