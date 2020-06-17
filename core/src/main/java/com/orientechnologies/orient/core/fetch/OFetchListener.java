/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.fetch;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Listener interface used while fetching records.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OFetchListener {
  /** Returns true if the listener fetches he fields. */
  boolean requireFieldProcessing();

  /**
   * Fetch the linked field.
   *
   * @param iRoot
   * @param iFieldName
   * @param iLinked
   * @return null if the fetching must stop, otherwise the current field value
   */
  Object fetchLinked(
      final ODocument iRoot,
      final Object iUserObject,
      final String iFieldName,
      final ODocument iLinked,
      final OFetchContext iContext)
      throws OFetchException;

  void parseLinked(
      final ODocument iRootRecord,
      final OIdentifiable iLinked,
      final Object iUserObject,
      final String iFieldName,
      final OFetchContext iContext)
      throws OFetchException;

  void parseLinkedCollectionValue(
      final ODocument iRootRecord,
      final OIdentifiable iLinked,
      final Object iUserObject,
      final String iFieldName,
      final OFetchContext iContext)
      throws OFetchException;

  Object fetchLinkedMapEntry(
      final ODocument iRoot,
      final Object iUserObject,
      final String iFieldName,
      final String iKey,
      final ODocument iLinked,
      final OFetchContext iContext)
      throws OFetchException;

  Object fetchLinkedCollectionValue(
      final ODocument iRoot,
      final Object iUserObject,
      final String iFieldName,
      final ODocument iLinked,
      final OFetchContext iContext)
      throws OFetchException;

  void processStandardField(
      final ODocument iRecord,
      final Object iFieldValue,
      final String iFieldName,
      final OFetchContext iContext,
      final Object iUserObject,
      String iFormat,
      OType filedType)
      throws OFetchException;

  void skipStandardField(
      final ODocument iRecord,
      final String iFieldName,
      final OFetchContext iContext,
      final Object iUserObject,
      String iFormat)
      throws OFetchException;
}
