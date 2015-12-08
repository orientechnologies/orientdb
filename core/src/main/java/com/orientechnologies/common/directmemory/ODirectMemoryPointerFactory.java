package com.orientechnologies.common.directmemory;

import com.orientechnologies.common.concur.lock.ODistributedCounter;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/8/2015
 */
public class ODirectMemoryPointerFactory extends NotificationBroadcasterSupport
    implements ODirectMemoryMXBean, OOrientStartupListener, OOrientShutdownListener {

  public static final String MBEAN_NAME = "com.orientechnologies.common.directmemory:type=ODirectMemoryMXBean";

  private static final ODirectMemoryPointerFactory INSTANCE = new ODirectMemoryPointerFactory();

  private final boolean       isTracked    = OGlobalConfiguration.DIRECT_MEMORY_TRACK_MODE.getValueAsBoolean();
  private final boolean       safeMode     = OGlobalConfiguration.DIRECT_MEMORY_SAFE_MODE.getValueAsBoolean();
  private final ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

  private final ODistributedCounter memorySize     = new ODistributedCounter();
  private final AtomicLong          sequenceNumber = new AtomicLong();

  private final AtomicBoolean mbeanIsRegistered = new AtomicBoolean();

  public static ODirectMemoryPointerFactory instance() {
    return INSTANCE;
  }

  public ODirectMemoryPointerFactory() {
    Orient.instance().registerWeakOrientShutdownListener(this);
    Orient.instance().registerWeakOrientStartupListener(this);

    try {
      registerMBean();
    } catch (RuntimeException e) {
      OLogManager.instance().error(this, "Error during registration of direct memory MBean", e);
    }

  }

  @Override
  public void onShutdown() {
    try {
      unregisterMBean();
    } catch (RuntimeException e) {
      OLogManager.instance().error(this, "Error during unregistration of direct memory MBean", e);
    }
  }

  @Override
  public void onStartup() {
    try {
      registerMBean();
    } catch (RuntimeException e) {
      OLogManager.instance().error(this, "Error during registration of direct memory MBean", e);
    }
  }

  public void registerMBean() {
    if (isTracked) {
      if (mbeanIsRegistered.compareAndSet(false, true)) {
        try {
          final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
          final ObjectName mbeanName = new ObjectName(MBEAN_NAME);
          server.registerMBean(this, mbeanName);
        } catch (MalformedObjectNameException e) {
          throw new IllegalStateException("Error during registration of direct memory MBean", e);
        } catch (InstanceAlreadyExistsException e) {
          throw new IllegalStateException("Error during registration of direct memory MBean", e);
        } catch (MBeanRegistrationException e) {
          throw new IllegalStateException("Error during registration of direct memory MBean", e);
        } catch (NotCompliantMBeanException e) {
          throw new IllegalStateException("Error during registration of direct memory MBean", e);
        }
      }
    }
  }

  public void unregisterMBean() {
    if (isTracked) {
      if (mbeanIsRegistered.compareAndSet(true, false)) {
        try {
          final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
          final ObjectName mbeanName = new ObjectName(MBEAN_NAME);
          server.unregisterMBean(mbeanName);
        } catch (MalformedObjectNameException e) {
          throw new IllegalStateException("Error during unregistration of direct memory MBean", e);
        } catch (InstanceNotFoundException e) {
          throw new IllegalStateException("Error during unregistration of direct memory MBean", e);
        } catch (MBeanRegistrationException e) {
          throw new IllegalStateException("Error during unregistration of direct memory MBean", e);
        }
      }
    }
  }

  public ODirectMemoryPointer createPointer(final byte[] data) {
    if (isTracked) {
      final ODirectMemoryPointer pointer = new OTrackedDirectMemoryPointer(data, this, safeMode, directMemory);
      memorySize.add(data.length);

      return pointer;
    }

    return new OUntrackedDirectMemoryPointer(data, safeMode, directMemory, this);
  }

  public ODirectMemoryPointer createPointer(final long pageSize) {
    if (isTracked) {
      final ODirectMemoryPointer pointer = new OTrackedDirectMemoryPointer(pageSize, this, safeMode, directMemory);
      memorySize.add(pageSize);

      return pointer;
    }

    return new OUntrackedDirectMemoryPointer(safeMode, directMemory, pageSize, this);
  }

  public void memoryLeakDetected(StackTraceElement[] allocationStackTrace) {
    final Notification notification = new Notification("com.orientechnologies.common.directmemory.memoryleak", MBEAN_NAME,
        sequenceNumber.getAndIncrement(), System.currentTimeMillis());
    notification.setUserData(allocationStackTrace);

    sendNotification(notification);
  }

  public void memoryFreed(final long pageSize) {
    memorySize.add(-pageSize);
  }

  @Override
  public long getSize() {
    return memorySize.get();
  }

  @Override
  public long getSizeInKB() {
    return getSize() / 1024;
  }

  @Override
  public long getSizeInMB() {
    return getSizeInKB() / 1024;
  }

  @Override
  public long getSizeInGB() {
    return getSizeInMB() / 1024;
  }
}
