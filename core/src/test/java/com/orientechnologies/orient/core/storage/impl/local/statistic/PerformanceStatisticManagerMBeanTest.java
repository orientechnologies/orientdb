package com.orientechnologies.orient.core.storage.impl.local.statistic;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import java.util.Arrays;
import java.util.HashSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test
public class PerformanceStatisticManagerMBeanTest {
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
    Assert.assertEquals(attributes.length, 15);
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

    Assert.assertTrue(found, "Operation with name " + name + " was not found");
  }

}
