/*
 *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more information: http://www.orientdb.com
 */

package com.orientechnologies.common.directmemory;

/**
 * Provides an MBean for {@link OByteBufferPool}.
 *
 * @author Sergey Sitnikov
 */
public interface OByteBufferPoolMXBean {

  /**
   * @return the buffer AKA page size in bytes of the associated {@link OByteBufferPool}.
   */
  int getBufferSize();

  /**
   * @return the number of the free buffers currently in the pool of the associated {@link OByteBufferPool}.
   */
  int getBuffersInThePool();

  /**
   * @return the number of the allocated buffers of the associated {@link OByteBufferPool},
   * this does not include the overflow buffers.
   */
  long getPreAllocatedBufferCount();

  /**
   * @return the number of the allocated overflow buffers of the associated {@link OByteBufferPool}.
   */
  long getOverflowBufferCount();

  /**
   * @return the current total memory allocation size in bytes of the the associated {@link OByteBufferPool},
   * including the overflow buffer allocations.
   */
  long getAllocatedMemory();

  /**
   * @return the current total memory allocation size in megabytes of the the associated {@link OByteBufferPool},
   * including the overflow buffer allocations.
   */
  long getAllocatedMemoryInMB();

  /**
   * @return the current total memory allocation size in gigabytes of the the associated {@link OByteBufferPool},
   * including the overflow buffer allocations.
   */
  double getAllocatedMemoryInGB();

  /**
   * @return Maximum amount of bytes which may be allocated using big chunks of memory
   */
  long getPreAllocationLimit();

  /**
   * @return Amount of pages which fit in single big chunk of memory preallocated during page allocation request.
   */
  int getMaxPagesPerSingleArea();

  /**
   * @return Size of the pool which contains records which were allocated but now were fried to the pool by database.
   */
  int getPoolSize();

  /**
   * Starts the tracing.
   */
  void startTracing();

  /**
   * Stops the tracing.
   */
  void stopTracing();

  /**
   * @return the active trace aggregation level.
   */
  OByteBufferPool.TraceAggregation getTraceAggregation();

  /**
   * Sets trace aggregation level.
   *
   * @param traceAggregation the new trace aggregation level.
   *
   * @see OByteBufferPool.TraceAggregation
   */
  void setTraceAggregation(OByteBufferPool.TraceAggregation traceAggregation);
}
