/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.storage.cache;

public class OAbstractWriteCache {
  public static long composeFileId(int storageId, int fileId) {
    return (((long) storageId) << 32) | fileId;
  }

  public static int extractFileId(long fileId) {
    return (int) (fileId & 0xFFFFFFFFL);
  }

  public static int extractStorageId(long fileId) {
    return (int) (fileId >>> 32);
  }

  public static long checkFileIdCompatibility(int storageId, long fileId) {
    if (extractStorageId(fileId) == 0) {
      return composeFileId(storageId, (int) fileId);
    }

    return fileId;
  }

}
