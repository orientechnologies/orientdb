/*
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *  For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.statistic;

import javax.management.*;
import javax.management.modelmbean.ModelMBeanAttributeInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * JMX bean which allows to start/stop monitoring of OrientDB performance
 * characteristics and exposes all system and component wide performance attributes of {@link OPerformanceStatisticManager}.
 * <p>
 * System wide attributes has the same MBean attribute names, but for each component wide attribute MBean attribute name consist of
 * "attribute name" {@link #COMPONENT_SEPARATOR} "component name".
 */
public class OPerformanceStatisticManagerMBean implements DynamicMBean {
  /**
   * Name of "pagesPerOperation" performance attribute
   */
  public static final String PAGES_PER_OPERATION = "pagesPerOperation";

  /**
   * Name of "cacheHits" performance attribute
   */
  public static final String CACHE_HITS = "cacheHits";

  /**
   * Separator in property name between component name and name of performance attribute
   */
  public static final String COMPONENT_SEPARATOR = "_";

  /**
   * Name of "commitTime" performance attribute
   */
  public static final String COMMIT_TIME = "commitTime";

  /**
   * Name of "readSpeedFromCache" performance attribute
   */
  public static final String READ_SPEED_FROM_CACHE = "readSpeedFromCache";

  /**
   * Name of "readSpeedFromFile" performance attribute
   */
  public static final String READ_SPEED_FROM_FILE = "readSpeedFromFile";

  /**
   * Name of "writeSpeedInCache" performance attribute
   */
  public static final String WRITE_SPEED_IN_CACHE = "writeSpeedInCache";

  /**
   * Name of "startMonitoring" method
   */
  public static final String START_MONITORING = "startMonitoring";

  /**
   * Name of "stopMonitoring" method
   */
  public static final String STOP_MONITORING = "stopMonitoring";

  /**
   * Name of "writeCachePagesPerFlush" performance attribute
   */
  public static final String WRITE_CACHE_PAGES_PER_FLUSH = "writeCachePagesPerFlush";

  /**
   * Name of "writeCacheFlushOperationsTime" performance attribute
   */
  public static final String WRITE_CACHE_FLUSH_OPERATION_TIME = "writeCacheFlushOperationTime";

  /**
   * Name of "writeCacheFuzzyCheckpointTime" performance attribute
   */
  public static final String WRITE_CACHE_FUZZY_CHECKPOINT_TIME = "writeCacheFuzzyCheckpointTime";

  /**
   * Name of "fullCheckpointTime" performance attribute
   */
  public static final String FULL_CHECKPOINT_TIME = "fullCheckpointTime";

  /**
   * Name of "fullCheckpointCount" performance attribute
   */
  public static final String FULL_CHECKPOINT_COUNT = "fullCheckpointCount";

  /**
   * Name of "readCacheSize" performance attribute
   */
  public static final String READ_CACHE_SIZE = "readCacheSize";

  /**
   * Name of "writeCacheSize" performance attribute
   */
  public static final String WRITE_CACHE_SIZE = "writeCacheSize";

  /**
   * Name of "exclusiveWriteCacheSize" performance attribute
   */
  public static final String EXCLUSIVE_WRITE_CACHE_SIZE = "exclusiveWriteCacheSize";

  /**
   * Name of "writeCacheOverflowCount" performance attribute
   */
  public static final String WRITE_CACHE_OVERFLOW_COUNT = "writeCacheOverflowCount";

  /**
   * Name of "walSize" performance attribute
   */
  public static final String WAL_SIZE = "walSize";

  /**
   * Name of "walCacheOverflowCount" performance attribute
   */
  public static final String WAL_CACHE_OVERFLOW_COUNT = "walCacheOverflowCount";

  /**
   * Name of "walLogTime" performance attribute
   */
  public static final String WAL_LOG_TIME = "walLogTime";

  /**
   * Name of "walStartAOLogTime" performance attribute
   */
  public static final String WAL_START_AO_LOG_TIME = "walStartAOLogTime";

  /**
   * Name of "walEndAOLogTime" performance attribute
   */
  public static final String WAL_END_AO_LOG_TIME = "walEndAOLogTime";

  /**
   * Name of "walFlushTime" performance attribute
   */
  public static final String WAL_FLUSH_TIME = "walFlushTime";

  /**
   * Reference to related performance manager
   */
  private final OPerformanceStatisticManager manager;

  public OPerformanceStatisticManagerMBean(OPerformanceStatisticManager manager) {
    this.manager = manager;
  }

  @Override
  public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
    if (attribute == null) {
      throw new RuntimeOperationsException(new IllegalArgumentException("Attribute name cannot be null"),
          "Cannot invoke a getter of " + getClass().getSimpleName() +
              " with null attribute name");
    }

    final int separatorIndex = attribute.indexOf(COMPONENT_SEPARATOR);

    final String attributeName;
    final String componentName;

    if (separatorIndex == 0) {
      throw new RuntimeOperationsException(new IllegalArgumentException("Empty attribute"),
          "Performance attribute name is not specified before " + COMPONENT_SEPARATOR + " for attribute " + attribute);
    }

    if (separatorIndex == attribute.length() - 1) {
      throw new RuntimeOperationsException(new IllegalArgumentException("Empty component"),
          "Component name is not specified after " + COMPONENT_SEPARATOR + " for attribute " + attribute);
    }

    if (separatorIndex > -1) {
      attributeName = attribute.substring(0, separatorIndex);
      componentName = attribute.substring(separatorIndex + 1);
    } else {
      attributeName = attribute;
      componentName = null;
    }

    //if attribute name does not include component name it is system wide not component wide attribute
    if (attributeName.equals(CACHE_HITS)) {
      if (componentName == null)
        return manager.getCacheHits();
      else {
        return manager.getCacheHits(componentName);
      }
    } else if (attributeName.equals(COMMIT_TIME)) {
      if (componentName == null)
        return manager.getCommitTime();
      else
        return throwComponentsAreNotSupported(COMMIT_TIME);
    } else if (attributeName.equals(READ_SPEED_FROM_CACHE)) {
      if (componentName == null)
        return manager.getReadSpeedFromCacheInPages();
      else
        return manager.getReadSpeedFromCacheInPages(componentName);
    } else if (attributeName.equals(READ_SPEED_FROM_FILE)) {
      if (componentName == null)
        return manager.getReadSpeedFromFileInPages();
      else
        return manager.getReadSpeedFromFileInPages(componentName);
    } else if (attributeName.equals(WRITE_SPEED_IN_CACHE)) {
      if (componentName == null)
        return manager.getWriteSpeedInCacheInPages();
      else
        return manager.getWriteSpeedInCacheInPages(componentName);
    } else if (attributeName.equals(PAGES_PER_OPERATION)) {
      if (componentName == null)
        throw new RuntimeOperationsException(new IllegalArgumentException("Unknown attribute"),
            "Amount of pages per operation is measured only on component level");

      return manager.getAmountOfPagesPerOperation(componentName);
    } else if (attributeName.equals(WRITE_CACHE_PAGES_PER_FLUSH)) {
      if (componentName == null)
        return manager.getWriteCachePagesPerFlush();
      else
        throwComponentsAreNotSupported(WRITE_CACHE_PAGES_PER_FLUSH);
    } else if (attributeName.equals(WRITE_CACHE_FLUSH_OPERATION_TIME)) {
      if (componentName == null)
        return manager.getWriteCacheFlushOperationsTime();
      else
        throwComponentsAreNotSupported(WRITE_CACHE_FLUSH_OPERATION_TIME);
    } else if (attributeName.equals(WRITE_CACHE_FUZZY_CHECKPOINT_TIME)) {
      if (componentName == null)
        return manager.getWriteCacheFuzzyCheckpointTime();
      else
        throwComponentsAreNotSupported(WRITE_CACHE_FUZZY_CHECKPOINT_TIME);
    } else if (attributeName.equals(FULL_CHECKPOINT_TIME)) {
      if (componentName == null)
        return manager.getFullCheckpointTime();
      else
        throwComponentsAreNotSupported(FULL_CHECKPOINT_TIME);
    } else if (attributeName.equals(FULL_CHECKPOINT_COUNT)) {
      if (componentName == null)
        return manager.getFullCheckpointCount();
      else
        throwComponentsAreNotSupported(FULL_CHECKPOINT_COUNT);
    } else if (attributeName.equals(READ_CACHE_SIZE)) {
      if (componentName == null)
        return manager.getReadCacheSize();
      else
        throwComponentsAreNotSupported(READ_CACHE_SIZE);
    } else if (attributeName.equals(WRITE_CACHE_SIZE)) {
      if (componentName == null)
        return manager.getWriteCacheSize();
      else
        throwComponentsAreNotSupported(WRITE_CACHE_SIZE);
    } else if (attributeName.equals(EXCLUSIVE_WRITE_CACHE_SIZE)) {
      if (componentName == null)
        return manager.getExclusiveWriteCacheSize();
      else
        throwComponentsAreNotSupported(EXCLUSIVE_WRITE_CACHE_SIZE);
    } else if (attributeName.equals(WRITE_CACHE_OVERFLOW_COUNT)) {
      if (componentName == null)
        return manager.getWriteCacheOverflowCount();
      else
        throwComponentsAreNotSupported(WAL_CACHE_OVERFLOW_COUNT);
    } else if (attributeName.equals(WAL_SIZE)) {
      if (componentName == null)
        return manager.getWALSize();
      else
        throwComponentsAreNotSupported(WAL_SIZE);
    } else if (attributeName.equals(WAL_CACHE_OVERFLOW_COUNT)) {
      if (componentName == null)
        return manager.getWALCacheOverflowCount();
      else
        throwComponentsAreNotSupported(WAL_CACHE_OVERFLOW_COUNT);
    } else if (attributeName.equals(WAL_LOG_TIME)) {
      if (componentName == null)
        return manager.getWALLogRecordTime();
      else
        throwComponentsAreNotSupported(WAL_LOG_TIME);
    } else if (attributeName.equals(WAL_START_AO_LOG_TIME)) {
      if (componentName == null)
        return manager.getWALStartAOLogRecordTime();
      else
        throwComponentsAreNotSupported(WAL_START_AO_LOG_TIME);
    } else if (attributeName.equals(WAL_END_AO_LOG_TIME)) {
      if (componentName == null)
        return manager.getWALStopAOLogRecordTime();
      else
        throwComponentsAreNotSupported(WAL_END_AO_LOG_TIME);
    } else if (attributeName.equals(WAL_FLUSH_TIME)) {
      if (componentName == null)
        return manager.getWALFlushTime();
      else
        throwComponentsAreNotSupported(WAL_FLUSH_TIME);
    }

    throw new AttributeNotFoundException("Cannot find " + attribute + " attribute in " + getClass().getSimpleName());
  }

  private Object throwComponentsAreNotSupported(String attributeName) {
    throw new RuntimeOperationsException(new IllegalArgumentException("Components are not supported"),
        attributeName + " attribute is not supported on component level");
  }

  @Override
  public void setAttribute(Attribute attribute)
      throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
    throwAreNotSupported();
  }

  private void throwAreNotSupported() {
    throw new RuntimeOperationsException(new UnsupportedOperationException("Modification operations are not supported"),
        "You can not apply modification operations to performance attributes");
  }

  @Override
  public AttributeList getAttributes(String[] attributes) {
    if (attributes == null) {
      throw new RuntimeOperationsException(new IllegalArgumentException("attributeNames[] cannot be null"),
          "Cannot invoke a getter of " + getClass().getSimpleName());
    }
    AttributeList resultList = new AttributeList();

    if (attributes.length == 0)
      return resultList;

    // build the result attribute list
    for (String attribute : attributes) {
      try {
        Object value = getAttribute(attribute);
        resultList.add(new Attribute(attribute, value));
      } catch (Exception e) {
        // print debug info but continue processing list
        e.printStackTrace();
      }
    }

    return resultList;
  }

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    throwAreNotSupported();
    return new AttributeList();
  }

  @Override
  public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
    if (actionName == null) {
      throw new RuntimeOperationsException(new IllegalArgumentException("Operation name cannot be null"),
          "Cannot invoke a null operation in " + getClass().getSimpleName());
    }

    if (actionName.equals(START_MONITORING)) {
      manager.startMonitoring();
      return null;
    }

    if (actionName.equals(STOP_MONITORING)) {
      manager.stopMonitoring();
      return null;
    }

    throw new ReflectionException(new NoSuchMethodException(actionName), "Cannot find the operation " + actionName +
        " in " + getClass().getSimpleName());
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    final List<MBeanAttributeInfo> performanceAttributes = new ArrayList<MBeanAttributeInfo>();
    populatePerformanceAttributes(performanceAttributes);

    final List<MBeanOperationInfo> operations = new ArrayList<MBeanOperationInfo>();

    final MBeanOperationInfo startMonitoring = new MBeanOperationInfo(START_MONITORING,
        "Starts monitoring OrientDB performance characteristics", new MBeanParameterInfo[0], void.class.getName(),
        MBeanOperationInfo.ACTION);
    operations.add(startMonitoring);

    final MBeanOperationInfo stopMonitoring = new MBeanOperationInfo(STOP_MONITORING,
        "Stops monitoring OrientDB performance characteristics", new MBeanParameterInfo[0], void.class.getName(),
        MBeanOperationInfo.ACTION);
    operations.add(stopMonitoring);

    return new MBeanInfo(this.getClass().getName(), "MBean to monitor OrientDB performance characteristics",
        performanceAttributes.toArray(new MBeanAttributeInfo[performanceAttributes.size()]), new MBeanConstructorInfo[0],
        operations.toArray(new MBeanOperationInfo[operations.size()]), new MBeanNotificationInfo[0]);
  }

  private void populatePerformanceAttributes(final List<MBeanAttributeInfo> performanceAttributes) {
    final Collection<String> components = manager.getComponentNames();

    populateCacheHits(performanceAttributes, components);
    populateCommitTime(performanceAttributes);
    populateReadSpeedFromCache(performanceAttributes, components);
    populateReadSpeedFromFile(performanceAttributes, components);
    populateWriteSpeedInCache(performanceAttributes, components);
    populatePagesPerOperation(performanceAttributes, components);

    populateWriteCachePagesPerFlush(performanceAttributes);
    populateWriteCacheFlushOperationsTime(performanceAttributes);
    populateWriteCacheFuzzyCheckpointTime(performanceAttributes);

    populateFullCheckpointTime(performanceAttributes);
    populateFullCheckpointCount(performanceAttributes);

    populateReadCacheSize(performanceAttributes);
    populateWriteCacheSize(performanceAttributes);
    populateExclusiveWriteCacheSize(performanceAttributes);
    populateWriteCacheOverflowCount(performanceAttributes);

    populateWALSize(performanceAttributes);
    populateWALCacheOverflowCount(performanceAttributes);
    populateWALLogTime(performanceAttributes);
    populateWALEndAOLogTime(performanceAttributes);
    populateWALStartAOLogTime(performanceAttributes);
    populateWALFlushTime(performanceAttributes);
  }

  private void populateWriteSpeedInCache(List<MBeanAttributeInfo> performanceAttributes, Collection<String> components) {
    final MBeanAttributeInfo writeSpeedInCache = new ModelMBeanAttributeInfo(WRITE_SPEED_IN_CACHE, long.class.getName(),
        "Write speed to disk cache in pages per second", true, false, false);
    performanceAttributes.add(writeSpeedInCache);

    for (String component : components) {
      final MBeanAttributeInfo componentWriteSpeedInCache = new ModelMBeanAttributeInfo(
          WRITE_SPEED_IN_CACHE + COMPONENT_SEPARATOR + component, long.class.getName(),
          "Write speed to disk cache in pages per second for component " + component, true, false, false);
      performanceAttributes.add(componentWriteSpeedInCache);
    }
  }

  private void populateReadSpeedFromFile(List<MBeanAttributeInfo> performanceAttributes, Collection<String> components) {
    final MBeanAttributeInfo readSpeedFromFile = new ModelMBeanAttributeInfo(READ_SPEED_FROM_FILE, long.class.getName(),
        "Read speed from file system in pages per second", true, false, false);
    performanceAttributes.add(readSpeedFromFile);

    for (String component : components) {
      final MBeanAttributeInfo componentReadSpeedFromFile = new ModelMBeanAttributeInfo(
          READ_SPEED_FROM_FILE + COMPONENT_SEPARATOR + component, long.class.getName(),
          "Read speed from file system in pages per second for component " + component, true, false, false);
      performanceAttributes.add(componentReadSpeedFromFile);
    }
  }

  private void populateReadSpeedFromCache(List<MBeanAttributeInfo> performanceAttributes, Collection<String> components) {
    final MBeanAttributeInfo readSpeedFromCache = new ModelMBeanAttributeInfo(READ_SPEED_FROM_CACHE, long.class.getName(),
        "Read speed from disk cache in pages per second", true, false, false);
    performanceAttributes.add(readSpeedFromCache);

    for (String component : components) {
      final MBeanAttributeInfo componentReadSpeedFromCache = new ModelMBeanAttributeInfo(
          READ_SPEED_FROM_CACHE + COMPONENT_SEPARATOR + component, long.class.getName(),
          "Read speed from disk cache in pages per second for component " + component, true, false, false);
      performanceAttributes.add(componentReadSpeedFromCache);
    }
  }

  private void populateCommitTime(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo commitTime = new ModelMBeanAttributeInfo(COMMIT_TIME, long.class.getName(),
        "Average commit time in nanoseconds", true, false, false);
    performanceAttributes.add(commitTime);
  }

  private void populateCacheHits(List<MBeanAttributeInfo> performanceAttributes, Collection<String> components) {
    final MBeanAttributeInfo cacheHits = new MBeanAttributeInfo(CACHE_HITS, int.class.getName(),
        "Cache hits of read disk cache in percents", true, false, false);
    performanceAttributes.add(cacheHits);

    for (String component : components) {
      final MBeanAttributeInfo componentCacheHits = new ModelMBeanAttributeInfo(CACHE_HITS + COMPONENT_SEPARATOR + component,
          int.class.getName(), "Cache hits of read disc cache for component " + component + " in percents", true, false, false);
      performanceAttributes.add(componentCacheHits);
    }
  }

  private void populatePagesPerOperation(List<MBeanAttributeInfo> performanceAttributes, Collection<String> components) {
    for (String component : components) {
      final MBeanAttributeInfo componentCacheHits = new ModelMBeanAttributeInfo(
          PAGES_PER_OPERATION + COMPONENT_SEPARATOR + component, int.class.getName(),
          "Average amount of pages per operation for component " + component, true, false, false);
      performanceAttributes.add(componentCacheHits);
    }
  }

  private void populateWriteCachePagesPerFlush(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo pagesPerFlush = new ModelMBeanAttributeInfo(WRITE_CACHE_PAGES_PER_FLUSH, long.class.getName(),
        "Amount of pages are flushed inside of write cache flush operation", true, false, false);

    performanceAttributes.add(pagesPerFlush);
  }

  private void populateWriteCacheFlushOperationsTime(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo flushOperationsTime = new ModelMBeanAttributeInfo(WRITE_CACHE_FLUSH_OPERATION_TIME,
        long.class.getName(), "Time which is spent on each flush operation", true, false, false);

    performanceAttributes.add(flushOperationsTime);
  }

  private void populateWriteCacheFuzzyCheckpointTime(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo fuzzyCheckpointTime = new ModelMBeanAttributeInfo(WRITE_CACHE_FUZZY_CHECKPOINT_TIME,
        long.class.getName(), "Time which is spent on each fuzzy checkpoint", true, false, false);

    performanceAttributes.add(fuzzyCheckpointTime);
  }

  private void populateFullCheckpointTime(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo fullCheckpointTime = new ModelMBeanAttributeInfo(FULL_CHECKPOINT_TIME, long.class.getName(),
        "Time which is spent on each full checkpoint", true, false, false);

    performanceAttributes.add(fullCheckpointTime);
  }

  private void populateFullCheckpointCount(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo fullCheckpointCount = new ModelMBeanAttributeInfo(FULL_CHECKPOINT_COUNT, long.class.getName(),
        "Amount of times full checkpoints were executed by storage", true, false, false);

    performanceAttributes.add(fullCheckpointCount);
  }

  private void populateReadCacheSize(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo readCacheSize = new ModelMBeanAttributeInfo(READ_CACHE_SIZE, long.class.getName(),
        "Size of read cache in bytes", true, false, false);

    performanceAttributes.add(readCacheSize);
  }

  private void populateWriteCacheSize(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo writeCacheSize = new ModelMBeanAttributeInfo(WRITE_CACHE_SIZE, long.class.getName(),
        "Size of write cache in bytes", true, false, false);

    performanceAttributes.add(writeCacheSize);
  }

  private void populateExclusiveWriteCacheSize(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo exclusiveWriteCacheSize = new ModelMBeanAttributeInfo(EXCLUSIVE_WRITE_CACHE_SIZE, long.class.getName(),
        "Size of exclusive part of write cache in bytes", true, false, false);

    performanceAttributes.add(exclusiveWriteCacheSize);
  }

  private void populateWriteCacheOverflowCount(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo writeCacheOverflowCount = new ModelMBeanAttributeInfo(WRITE_CACHE_OVERFLOW_COUNT, long.class.getName(),
        "Count of times when there was not enough space in write cache to keep already written data", true, false, false);

    performanceAttributes.add(writeCacheOverflowCount);
  }

  private void populateWALSize(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo walSize = new ModelMBeanAttributeInfo(WAL_SIZE, long.class.getName(), "WAL size in bytes", true, false,
        false);

    performanceAttributes.add(walSize);
  }

  private void populateWALCacheOverflowCount(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo walCacheOverflowCount = new ModelMBeanAttributeInfo(WAL_CACHE_OVERFLOW_COUNT, long.class.getName(),
        "Count of times when there was not enough space in WAL to keep already written data", true, false, false);

    performanceAttributes.add(walCacheOverflowCount);
  }

  private void populateWALLogTime(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo walLogTime = new ModelMBeanAttributeInfo(WAL_LOG_TIME, long.class.getName(),
        "Time which is spent to log single record in WAL", true, false, false);

    performanceAttributes.add(walLogTime);
  }

  private void populateWALEndAOLogTime(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo walLogTime = new ModelMBeanAttributeInfo(WAL_END_AO_LOG_TIME, long.class.getName(),
        "Time which is spent to log record which indicates end of atomic operation in WAL", true, false, false);

    performanceAttributes.add(walLogTime);
  }

  private void populateWALStartAOLogTime(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo walLogTime = new ModelMBeanAttributeInfo(WAL_START_AO_LOG_TIME, long.class.getName(),
        "Time which is spent to log record which indicates start of atomic operation in WAL", true, false, false);

    performanceAttributes.add(walLogTime);
  }

  private void populateWALFlushTime(List<MBeanAttributeInfo> performanceAttributes) {
    final MBeanAttributeInfo walFlushTime = new ModelMBeanAttributeInfo(WAL_FLUSH_TIME, long.class.getName(),
        "Time which is spent on flush of WAL cache", true, false, false);

    performanceAttributes.add(walFlushTime);
  }

}
