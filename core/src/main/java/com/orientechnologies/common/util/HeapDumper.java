package com.orientechnologies.common.util;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;

public class HeapDumper {
  // This is the name of the HotSpot Diagnostic MBean
  private static final String                     HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";

  /**
   * Invoke {@code dumpHeap} operation on {@code com.sun.management:type=HotSpotDiagnostic} mbean.
   */
  public static void dumpHeap(String fileName, boolean live) {
    try {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      server.invoke(new ObjectName(HOTSPOT_BEAN_NAME),
          "dumpHeap",
          new Object[]{fileName, live},
          new String[]{String.class.getName(), Boolean.TYPE.getName()}
      );
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception exp) {
      throw new RuntimeException(exp);
    }
  }
}
