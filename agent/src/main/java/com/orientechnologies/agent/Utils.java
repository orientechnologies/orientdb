package com.orientechnologies.agent;

import com.orientechnologies.common.log.OLogManager;

import java.io.Closeable;

public class Utils {

  public static void safeClose(Object owner, Closeable... streams) {
    if (streams != null) {
      for (Closeable closeable : streams) {
        if (closeable != null) {
          try {
            closeable.close();
          } catch (Exception e) {
            OLogManager.instance().info(owner, "Failed to close output stream " + closeable);
          }
        }
      }
    }
  }

}
