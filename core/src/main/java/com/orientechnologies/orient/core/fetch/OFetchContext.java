/*
 *
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.core.fetch;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public interface OFetchContext {
  void onBeforeFetch(final ODocument iRootRecord) throws OFetchException;

  void onAfterFetch(final ODocument iRootRecord) throws OFetchException;

  void onBeforeArray(
      final ODocument iRootRecord,
      final String iFieldName,
      final Object iUserObject,
      final OIdentifiable[] iArray)
      throws OFetchException;

  void onAfterArray(final ODocument iRootRecord, final String iFieldName, final Object iUserObject)
      throws OFetchException;

  void onBeforeCollection(
      final ODocument iRootRecord,
      final String iFieldName,
      final Object iUserObject,
      final Iterable<?> iterable)
      throws OFetchException;

  void onAfterCollection(
      final ODocument iRootRecord, final String iFieldName, final Object iUserObject)
      throws OFetchException;

  void onBeforeMap(final ODocument iRootRecord, final String iFieldName, final Object iUserObject)
      throws OFetchException;

  void onAfterMap(final ODocument iRootRecord, final String iFieldName, final Object iUserObject)
      throws OFetchException;

  void onBeforeDocument(
      final ODocument iRecord,
      final ODocument iDocument,
      final String iFieldName,
      final Object iUserObject)
      throws OFetchException;

  void onAfterDocument(
      final ODocument iRootRecord,
      final ODocument iDocument,
      final String iFieldName,
      final Object iUserObject)
      throws OFetchException;

  void onBeforeStandardField(
      final Object iFieldValue, final String iFieldName, final Object iUserObject, OType fieldType);

  void onAfterStandardField(
      final Object iFieldValue, final String iFieldName, final Object iUserObject, OType fieldType);

  boolean fetchEmbeddedDocuments();
}
