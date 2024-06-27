package com.orientechnologies.agent;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import java.io.Closeable;

public class Utils {
  private static final OLogger logger = OLogManager.instance().logger(Utils.class);

  public static void safeClose(Object owner, Closeable... streams) {
    if (streams != null) {
      for (Closeable closeable : streams) {
        if (closeable != null) {
          try {
            closeable.close();
          } catch (Exception e) {
            logger.info("Failed to close output stream " + closeable);
          }
        }
      }
    }
  }
}
