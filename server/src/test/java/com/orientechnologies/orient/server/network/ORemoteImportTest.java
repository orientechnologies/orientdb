package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import org.junit.Test;

/** Created by tglman on 19/07/16. */
public class ORemoteImportTest extends BaseServerMemoryDatabase {
  @Test
  public void testImport() throws UnsupportedEncodingException {

    String content =
        "{\"records\": [{\"@type\": \"d\", \"@rid\": \"#9:0\",\"@version\": 1,\"@class\": \"V\"}]}";

    OStorageRemote storage = (OStorageRemote) db.getStorage();
    final StringBuffer buff = new StringBuffer();
    storage.importDatabase(
        "-merge=true",
        new ByteArrayInputStream(content.getBytes("UTF8")),
        "data.json",
        new OCommandOutputListener() {
          @Override
          public void onMessage(String iText) {
            buff.append(iText);
          }
        });
    assertTrue(buff.toString().contains("Database import completed"));
  }
}
