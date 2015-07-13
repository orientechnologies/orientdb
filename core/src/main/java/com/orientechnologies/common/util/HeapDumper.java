package com.orientechnologies.common.util;

import com.sun.management.HotSpotDiagnosticMXBean;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;

public class HeapDumper {
  // This is the name of the HotSpot Diagnostic MBean
  private static final String                     HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";

  // field to store the hotspot diagnostic MBean
  private static volatile HotSpotDiagnosticMXBean hotspotMBean;

  public static void dumpHeap(String fileName, boolean live) {
    // initialize hotspot diagnostic MBean
    initHotspotMBean();
    try {
      hotspotMBean.dumpHeap(fileName, live);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception exp) {
      throw new RuntimeException(exp);
    }
  }

  // initialize the hotspot diagnostic MBean field
  private static void initHotspotMBean() {
    if (hotspotMBean == null) {
      synchronized (HeapDumper.class) {
        if (hotspotMBean == null) {
          hotspotMBean = getHotspotMBean();
        }
      }
    }
  }

  // get the hotspot diagnostic MBean from the
  // platform MBean server
  private static HotSpotDiagnosticMXBean getHotspotMBean() {
    try {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      HotSpotDiagnosticMXBean bean = ManagementFactory.newPlatformMXBeanProxy(server, HOTSPOT_BEAN_NAME,
          HotSpotDiagnosticMXBean.class);
      return bean;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception exp) {
      throw new RuntimeException(exp);
    }
  }
}
