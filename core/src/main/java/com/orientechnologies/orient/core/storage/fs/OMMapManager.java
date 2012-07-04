/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.storage.fs;

/**
 * @author Artem Loginov (logart) logart2007@gmail.com Date: 5/17/12 Time: 5:24 PM
 *         <p/>
 *         This is universal MMapManager interface. If you would like implement your version of mmap manager, you should implement
 *         this interface.
 */
public interface OMMapManager {

  /**
   * This method used to initialize manager after creation. Constructor should be empty. Init method is called after creation an
   * manager so you can do all initialization steps in this method.
   */
  void init();

  /**
   * Types of operation on files that was mmapped.
   */
  public enum OPERATION_TYPE {
    READ, WRITE
  }

  /**
   * Strategy that determine how mmap should work.
   */
  public enum ALLOC_STRATEGY {
    MMAP_ALWAYS, MMAP_WRITE_ALWAYS_READ_IF_AVAIL_POOL, MMAP_WRITE_ALWAYS_READ_IF_IN_MEM, MMAP_ONLY_AVAIL_POOL, MMAP_NEVER
  }

  /**
   * This method tries to mmap file. If mapping is impossible method returns null.
   * 
   * @param iFile
   *          file that will be mmapped.
   * @param iBeginOffset
   *          position in file that should be mapped.
   * @param iSize
   *          size that should be mapped.
   * @param iOperationType
   *          operation (read/write) that will be performed on file.
   * @param iStrategy
   *          allocation strategy. @see com.orientechnologies.orient.core.storage.fs.OMMapManager.ALLOC_STRATEGY
   * @return array of entries that represent mapped files as byte buffers. Or null on error.
   */
  OMMapBufferEntry[] acquire(OFileMMap iFile, long iBeginOffset, int iSize, OPERATION_TYPE iOperationType, ALLOC_STRATEGY iStrategy);

  /**
   * This method call unlock on all mapped entries from array.
   * 
   * @param entries
   *          that will be unlocked.
   */
  void release(OMMapBufferEntry[] entries, OPERATION_TYPE operationType);

  /**
   * This method should flush all the records in all files on disk.
   */
  void flush();

  /**
   * This method close file flush it's content and remove information about file from manager.
   * 
   * @param iFile
   *          that will be removed
   */
  void removeFile(OFileMMap iFile);

  /**
   * This method is the same as flush but on single file. This method store all file content on disk from memory to prevent data
   * loosing.
   * 
   * @param iFile
   *          that will be flushed.
   */
  void flushFile(OFileMMap iFile);

  /**
   * This method flush all files and clear information about mmap from itself.
   */
  void shutdown();
}
