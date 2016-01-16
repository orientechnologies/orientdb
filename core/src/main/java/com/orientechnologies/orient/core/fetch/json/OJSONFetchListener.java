/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
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
package com.orientechnologies.orient.core.fetch.json;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;

import java.io.IOException;

/**
 * @author luca.molino
 */
public class OJSONFetchListener implements OFetchListener {

  public void processStandardField(final ODocument iRecord, final Object iFieldValue, final String iFieldName, final OFetchContext iContext, final Object iusObject, final String iFormat) {
    try {
      ((OJSONFetchContext) iContext).getJsonWriter().writeAttribute(((OJSONFetchContext) iContext).getIndentLevel() + 1, true, iFieldName, iFieldValue);
    } catch (IOException e) {
      throw new OFetchException("Error processing field '" + iFieldValue + " of record " + iRecord.getIdentity(), e);
    }
  }

  public void processStandardCollectionValue(final Object iFieldValue, final OFetchContext iContext) throws OFetchException {
    try {
      ((OJSONFetchContext) iContext).getJsonWriter().writeValue(((OJSONFetchContext) iContext).getIndentLevel(), true, OJSONWriter.encode(iFieldValue));
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on processStandardCollectionValue", e);
    }
  }

  public Object fetchLinked(final ODocument iRecord, final Object iUserObject, final String iFieldName, final ODocument iLinked, final OFetchContext iContext) throws OFetchException {
    return iLinked;
  }

  public Object fetchLinkedMapEntry(final ODocument iRecord, final Object iUserObject, final String iFieldName, final String iKey, final ODocument iLinked, final OFetchContext iContext) throws OFetchException {
    return iLinked;
  }

  public void parseLinked(final ODocument iRootRecord, final OIdentifiable iLinked, final Object iUserObject, final String iFieldName, final OFetchContext iContext) throws OFetchException {
    try {
      ((OJSONFetchContext) iContext).writeLinkedAttribute(iLinked, iFieldName);
    } catch (IOException e) {
      throw new OFetchException("Error writing linked field " + iFieldName + " (record:" + iLinked.getIdentity() + ") of record " + iRootRecord.getIdentity(), e);
    }
  }

  public void parseLinkedCollectionValue(ODocument iRootRecord, OIdentifiable iLinked, Object iUserObject, String iFieldName, OFetchContext iContext) throws OFetchException {
    try {
      if (((OJSONFetchContext) iContext).isInCollection(iRootRecord)) {
        ((OJSONFetchContext) iContext).writeLinkedValue(iLinked, iFieldName);
      } else {
        ((OJSONFetchContext) iContext).writeLinkedAttribute(iLinked, iFieldName);
      }
    } catch (IOException e) {
      throw new OFetchException("Error writing linked field " + iFieldName + " (record:" + iLinked.getIdentity() + ") of record " + iRootRecord.getIdentity(), e);
    }
  }

  public Object fetchLinkedCollectionValue(ODocument iRoot, Object iUserObject, String iFieldName, ODocument iLinked, OFetchContext iContext) throws OFetchException {
    return iLinked;
  }

  @Override
  public void skipStandardField(ODocument iRecord, String iFieldName, OFetchContext iContext, Object iUserObject, String iFormat) throws OFetchException {
  }
}
