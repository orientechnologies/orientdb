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
package com.orientechnologies.orient.core.fetch.remote;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Fetch context for {@class ONetworkBinaryProtocol} class
 *
 * @author Luca Molino (molino.luca--at--gmail.com)
 */
public class ORemoteFetchContext implements OFetchContext {
  public void onBeforeStandardField(
      Object iFieldValue, String iFieldName, Object iUserObject, OType fieldType) {}

  public void onAfterStandardField(
      Object iFieldValue, String iFieldName, Object iUserObject, OType fieldType) {}

  public void onBeforeMap(ODocument iRootRecord, String iFieldName, final Object iUserObject)
      throws OFetchException {}

  public void onBeforeFetch(ODocument iRootRecord) throws OFetchException {}

  public void onBeforeArray(
      ODocument iRootRecord, String iFieldName, Object iUserObject, OIdentifiable[] iArray)
      throws OFetchException {}

  public void onAfterArray(ODocument iRootRecord, String iFieldName, Object iUserObject)
      throws OFetchException {}

  public void onBeforeDocument(
      ODocument iRecord, final ODocument iDocument, String iFieldName, final Object iUserObject)
      throws OFetchException {}

  public void onBeforeCollection(
      ODocument iRootRecord,
      String iFieldName,
      final Object iUserObject,
      final Iterable<?> iterable)
      throws OFetchException {}

  public void onAfterMap(ODocument iRootRecord, String iFieldName, final Object iUserObject)
      throws OFetchException {}

  public void onAfterFetch(ODocument iRootRecord) throws OFetchException {}

  public void onAfterDocument(
      ODocument iRootRecord, final ODocument iDocument, String iFieldName, final Object iUserObject)
      throws OFetchException {}

  public void onAfterCollection(ODocument iRootRecord, String iFieldName, final Object iUserObject)
      throws OFetchException {}

  public boolean fetchEmbeddedDocuments() {
    return false;
  }
}
