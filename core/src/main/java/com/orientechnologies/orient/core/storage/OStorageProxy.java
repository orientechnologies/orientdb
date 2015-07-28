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
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Tagged interface for proxy storage implementation
 * 
 * @author Luca Garulli
 * 
 */
public interface OStorageProxy extends OStorage {
  String getUserName();

  Object indexGet(final String iIndexName, final Object iKey, final String iFetchPlan);

  void indexPut(final String iIndexName, final Object iKey, final OIdentifiable iValue);

  boolean indexRemove(final String iIndexName, final Object iKey);

  int getUsers();

  int addUser();

  int removeUser();
}
