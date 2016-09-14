package com.orientechnologies.orient.core.storage.impl.local.statistic;

import org.junit.Assert;
import org.junit.Test;

import javax.management.*;
import java.util.Arrays;
import java.util.HashSet;

import static org.mockito.Mockito.*;

public class PerformanceStatisticManagerMBeanTest {

  @Test
  public void testMbeanInfo() {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);
    final MBeanInfo mBeanInfo = mBean.getMBeanInfo();

    final MBeanOperationInfo[] operations = mBeanInfo.getOperations();
    Assert.assertEquals(operations.length, 2);

    assertOperation(operations, "startMonitoring");
    assertOperation(operations, "stopMonitoring");

    final MBeanAttributeInfo[] attributes = mBeanInfo.getAttributes();
    Assert.assertEquals(attributes.length, 30);

    assertAttribute(attributes, "cacheHits", int.class);
    assertAttribute(attributes, "cacheHits_com1", int.class);
    assertAttribute(attributes, "cacheHits_com2", int.class);

    assertAttribute(attributes, "commitTime", long.class);

    assertAttribute(attributes, "readSpeedFromCache", long.class);
    assertAttribute(attributes, "readSpeedFromCache_com1", long.class);
    assertAttribute(attributes, "readSpeedFromCache_com2", long.class);

    assertAttribute(attributes, "readSpeedFromFile", long.class);
    assertAttribute(attributes, "readSpeedFromFile_com1", long.class);
    assertAttribute(attributes, "readSpeedFromFile_com2", long.class);

    assertAttribute(attributes, "writeSpeedInCache", long.class);
    assertAttribute(attributes, "writeSpeedInCache_com1", long.class);
    assertAttribute(attributes, "writeSpeedInCache_com2", long.class);

    assertAttribute(attributes, "writeCachePagesPerFlush", long.class);
    assertAttribute(attributes, "writeCacheFlushOperationTime", long.class);
    assertAttribute(attributes, "writeCacheFuzzyCheckpointTime", long.class);

    assertAttribute(attributes, "fullCheckpointTime", long.class);

    Assert.assertEquals(mBeanInfo.getConstructors().length, 0);
    Assert.assertEquals(mBeanInfo.getNotifications().length, 0);
  }

  public void testCacheHits() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    when(manager.getCacheHits()).thenReturn(12);
    when(manager.getCacheHits("com1")).thenReturn(15);
    when(manager.getCacheHits("com2")).thenReturn(20);
    when(manager.getCacheHits("fd")).thenReturn(-1);

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);

    Integer result = (Integer) mBean.getAttribute("cacheHits");
    Assert.assertEquals((int) result, 12);

    result = (Integer) mBean.getAttribute("cacheHits_com1");
    Assert.assertEquals((int) result, 15);

    result = (Integer) mBean.getAttribute("cacheHits_com2");
    Assert.assertEquals((int) result, 20);

    verify(manager).getCacheHits();
    verify(manager).getCacheHits("com1");
    verify(manager).getCacheHits("com2");
    verifyNoMoreInteractions(manager);

    result = (Integer) mBean.getAttribute("cacheHits_fd");
    Assert.assertEquals((int) result, -1);

    try {
      mBean.getAttribute("dfd");
      Assert.fail();
    } catch (AttributeNotFoundException e) {
    }
  }

  public void testReadSpeedFromCache() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    when(manager.getReadSpeedFromCacheInPages()).thenReturn(120L);
    when(manager.getReadSpeedFromCacheInPages("com1")).thenReturn(150L);
    when(manager.getReadSpeedFromCacheInPages("com2")).thenReturn(200L);
    when(manager.getReadSpeedFromCacheInPages("fd")).thenReturn(-1L);

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);

    Long result = (Long) mBean.getAttribute("readSpeedFromCache");
    Assert.assertEquals((long) result, 120L);

    result = (Long) mBean.getAttribute("readSpeedFromCache_com1");
    Assert.assertEquals((long) result, 150);

    result = (Long) mBean.getAttribute("readSpeedFromCache_com2");
    Assert.assertEquals((long) result, 200);

    verify(manager).getReadSpeedFromCacheInPages();
    verify(manager).getReadSpeedFromCacheInPages("com1");
    verify(manager).getReadSpeedFromCacheInPages("com2");
    verifyNoMoreInteractions(manager);

    result = (Long) mBean.getAttribute("readSpeedFromCache_fd");
    Assert.assertEquals((long) result, -1);
  }

  public void testNullPointerAttribute() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);
    try {
      mBean.getAttribute(null);
      Assert.fail();
    } catch (RuntimeOperationsException e) {
    }
  }

  public void testWrongFormatAttribute() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));
    when(manager.getCacheHits("adsf_sda")).thenReturn(0);

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);
    mBean.getAttribute("cacheHits_adsf_sda");

    verify(manager).getCacheHits("adsf_sda");
    verifyNoMoreInteractions(manager);
  }

  public void testEmptyComponent() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);

    try {
      mBean.getAttribute("cacheHits_");
      Assert.fail();
    } catch (RuntimeOperationsException e) {
    }
  }

  public void testEmptyAttribute() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);

    try {
      mBean.getAttribute("_adsf");
      Assert.fail();
    } catch (RuntimeOperationsException e) {
    }
  }

  public void testPagesPerOperation() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    when(manager.getAmountOfPagesPerOperation("com1")).thenReturn(150L);
    when(manager.getAmountOfPagesPerOperation("com2")).thenReturn(200L);
    when(manager.getAmountOfPagesPerOperation("fd")).thenReturn(-1L);

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);
    try {
      mBean.getAttribute("pagesPerOperation");
      Assert.fail();
    } catch (RuntimeOperationsException e) {
    }

    Long result = (Long) mBean.getAttribute("pagesPerOperation_com1");
    Assert.assertEquals((long) result, 150);

    result = (Long) mBean.getAttribute("pagesPerOperation_com2");
    Assert.assertEquals((long) result, 200);

    verify(manager).getAmountOfPagesPerOperation("com1");
    verify(manager).getAmountOfPagesPerOperation("com2");
    verifyNoMoreInteractions(manager);

    result = (Long) mBean.getAttribute("pagesPerOperation_fd");
    Assert.assertEquals((long) result, -1);
  }

  public void testReadSpeedFromFile() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    when(manager.getReadSpeedFromFileInPages()).thenReturn(120L);
    when(manager.getReadSpeedFromFileInPages("com1")).thenReturn(150L);
    when(manager.getReadSpeedFromFileInPages("com2")).thenReturn(200L);
    when(manager.getReadSpeedFromFileInPages("fd")).thenReturn(-1L);

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);

    Long result = (Long) mBean.getAttribute("readSpeedFromFile");
    Assert.assertEquals((long) result, 120L);

    result = (Long) mBean.getAttribute("readSpeedFromFile_com1");
    Assert.assertEquals((long) result, 150);

    result = (Long) mBean.getAttribute("readSpeedFromFile_com2");
    Assert.assertEquals((long) result, 200);

    verify(manager).getReadSpeedFromFileInPages();
    verify(manager).getReadSpeedFromFileInPages("com1");
    verify(manager).getReadSpeedFromFileInPages("com2");
    verifyNoMoreInteractions(manager);

    result = (Long) mBean.getAttribute("readSpeedFromFile_fd");
    Assert.assertEquals((long) result, -1);
  }

  public void testWriteSpeedInCache() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    when(manager.getWriteSpeedInCacheInPages()).thenReturn(120L);
    when(manager.getWriteSpeedInCacheInPages("com1")).thenReturn(150L);
    when(manager.getWriteSpeedInCacheInPages("com2")).thenReturn(200L);
    when(manager.getWriteSpeedInCacheInPages("fd")).thenReturn(-1L);

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);

    Long result = (Long) mBean.getAttribute("writeSpeedInCache");
    Assert.assertEquals((long) result, 120L);

    result = (Long) mBean.getAttribute("writeSpeedInCache_com1");
    Assert.assertEquals((long) result, 150);

    result = (Long) mBean.getAttribute("writeSpeedInCache_com2");
    Assert.assertEquals((long) result, 200);

    verify(manager).getWriteSpeedInCacheInPages();
    verify(manager).getWriteSpeedInCacheInPages("com1");
    verify(manager).getWriteSpeedInCacheInPages("com2");
    verifyNoMoreInteractions(manager);

    result = (Long) mBean.getAttribute("writeSpeedInCache_fd");
    Assert.assertEquals((long) result, -1);
  }

  public void testCommitTime() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    when(manager.getCommitTime()).thenReturn(200L);

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);

    Long result = (Long) mBean.getAttribute("commitTime");
    Assert.assertEquals((long) result, 200L);

    verify(manager).getCommitTime();
    verifyNoMoreInteractions(manager);

    try {
      mBean.getAttribute("commitTime_fd");
      Assert.fail();
    } catch (RuntimeOperationsException e) {
    }
  }

  public void testStartMonitoring() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);
    mBean.invoke("startMonitoring", new Object[0], new String[0]);

    verify(manager).startMonitoring();
    verifyNoMoreInteractions(manager);
  }

  public void testStopMonitoring() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);
    mBean.invoke("stopMonitoring", new Object[0], new String[0]);

    verify(manager).stopMonitoring();
    verifyNoMoreInteractions(manager);
  }

  public void testNullMethod() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);
    try {
      mBean.invoke(null, new Object[0], new String[0]);
      Assert.fail();
    } catch (RuntimeOperationsException e) {
    }
  }

  public void testIncorectMethodName() throws Exception {
    final OPerformanceStatisticManager manager = mock(OPerformanceStatisticManager.class);
    when(manager.getComponentNames()).thenReturn(new HashSet<String>(Arrays.asList("com1", "com2")));

    final OPerformanceStatisticManagerMBean mBean = new OPerformanceStatisticManagerMBean(manager);
    try {
      mBean.invoke("adfd", new Object[0], new String[0]);
      Assert.fail();
    } catch (ReflectionException e) {
    }
  }

  private void assertOperation(MBeanOperationInfo[] operations, String name) {
    boolean found = false;

    for (MBeanOperationInfo operationInfo : operations) {
      if (operationInfo.getName().equals(name)) {
        Assert.assertEquals(operationInfo.getImpact(), MBeanOperationInfo.ACTION);
        Assert.assertEquals(operationInfo.getReturnType(), void.class.getName());
        Assert.assertEquals(operationInfo.getSignature(), new MBeanParameterInfo[0]);
        found = true;
      }
    }

    Assert.assertTrue("Operation with name " + name + " was not found", found);
  }

  private void assertAttribute(MBeanAttributeInfo[] attributes, String name, Class clazz) {
    boolean found = false;
    for (MBeanAttributeInfo actual : attributes) {
      if (actual.getName().equals(name)) {
        Assert.assertEquals(actual.getType(), clazz.toString());
        Assert.assertEquals(actual.isIs(), false);
        Assert.assertEquals(actual.isReadable(), true);
        Assert.assertEquals(actual.isWritable(), false);
        found = true;
      }
    }

    Assert.assertTrue("Property with name " + name + " was not found", found);
  }

}
