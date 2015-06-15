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
package com.orientechnologies.orient.core.cache;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Global cache contained in the resource space of OStorage and shared by all database instances that work on top of that Storage.
 * 
 * @author Luca Garulli
 */
public interface OGlobalRecordCache {
  int getSize();

  /**
   * Pushes record to cache. Identifier of record used as access key
   *
   * @param record
   *          record that should be cached
   */
  void updateRecord(final ORecord record);

  ORecord findRecord(final ORID rid);

  void deleteRecord(final ORID rid);

  void clear();
}
