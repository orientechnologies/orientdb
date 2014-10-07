package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Field;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/7/14
 */
@Test
public class LocalPaginatedStorageTest {
  public void testReadOnlyMode() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    buildDirectory += "/readOnlyModeTest";

    final File dir = new File(buildDirectory);

    final long freeSpace = dir.getFreeSpace();
    final byte[] data = new byte[1 << 7];
    final long feeSpaceLimit = freeSpace - (OGlobalConfiguration.WAL_MAX_SIZE.getValueAsInteger() * 1024L * 1024L) - 1024L;

    final long oldSpaceLimit = OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getValueAsLong();
    final int oldCheckInterval = OGlobalConfiguration.DISC_CACHE_FREE_SPACE_CHECK_INTERVAL.getValueAsInteger();

    OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.setValue(feeSpaceLimit / (1024L * 1024L));
    OGlobalConfiguration.DISC_CACHE_FREE_SPACE_CHECK_INTERVAL.setValue(1);

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + buildDirectory);
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    for (int i = 0; i < 8; i++) {
      ODocument document = new ODocument();
      document.field("data", data);
      document.save();
    }

    Thread.sleep(2000);

		for (int i = 0; i < 2; i++) {
			ODocument document = new ODocument();
			document.field("data", data);
			document.save();
		}

  }
}