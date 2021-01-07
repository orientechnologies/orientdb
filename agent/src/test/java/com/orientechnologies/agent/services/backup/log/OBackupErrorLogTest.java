package com.orientechnologies.agent.services.backup.log;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.handler.OAutomaticBackup;
import org.junit.Assert;
import org.junit.Test;

public class OBackupErrorLogTest {

  @Test
  public void testFromToDoc() {
    OBackupErrorLog log =
        new OBackupErrorLog(1, 2, "abcd", "adsf", OAutomaticBackup.MODE.FULL_BACKUP.name());
    ODocument doc = log.toDoc();
    OBackupErrorLog log2 =
        new OBackupErrorLog(2, 3, "aaabcd", "adccsf", OAutomaticBackup.MODE.FULL_BACKUP.name());

    log2.fromDoc(doc);

    Assert.assertEquals(doc.toJSON(), log2.toDoc().toJSON());
  }
}
