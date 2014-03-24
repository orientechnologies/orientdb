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
package com.orientechnologies.orient.core.fetch.remote;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

import java.util.Collection;
import java.util.Map;

/**
 * Fetch listener for {@class ONetworkBinaryProtocol} class
 * 
 * Whenever a record has to be fetched it will be added to the list of records to send
 * 
 * @author luca.molino
 * 
 */
public abstract class ORemoteFetchListener implements OFetchListener {

  public ORemoteFetchListener() {
  }

  protected abstract void sendRecord(ORecord<?> iLinked);

  public void processStandardField(ORecordSchemaAware<?> iRecord, Object iFieldValue, String iFieldName, OFetchContext iContext,
      final Object iusObject, final String iFormat) throws OFetchException {
  }

  public void parseLinked(ORecordSchemaAware<?> iRootRecord, OIdentifiable iLinked, Object iUserObject, String iFieldName,
      OFetchContext iContext) throws OFetchException {
  }

  public void parseLinkedCollectionValue(ORecordSchemaAware<?> iRootRecord, OIdentifiable iLinked, Object iUserObject,
      String iFieldName, OFetchContext iContext) throws OFetchException {
  }

  public Object fetchLinkedMapEntry(ORecordSchemaAware<?> iRoot, Object iUserObject, String iFieldName, String iKey,
      ORecordSchemaAware<?> iLinked, OFetchContext iContext) throws OFetchException {
    if (iLinked.getIdentity().isValid()) {
      sendRecord(iLinked);
      return true;
    }
    return null;
  }

  public Object fetchLinkedCollectionValue(ORecordSchemaAware<?> iRoot, Object iUserObject, String iFieldName,
      ORecordSchemaAware<?> iLinked, OFetchContext iContext) throws OFetchException {
    if (iLinked.getIdentity().isValid()) {
      sendRecord(iLinked);
      return true;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public Object fetchLinked(ORecordSchemaAware<?> iRoot, Object iUserObject, String iFieldName, ORecordSchemaAware<?> iLinked,
      OFetchContext iContext) throws OFetchException {
    if (iLinked instanceof ORecord<?>) {
      sendRecord(iLinked);
      return true;
    }
    // HOW THIS CODE CAN HAVE SENSE?
    else if (iLinked instanceof Collection<?>) {
      for (ORecord<?> rec : (Collection<ORecord<?>>) iLinked)
        sendRecord(rec);
      return true;
    }
    // HOW THIS CODE CAN HAVE SENSE?
    else if (iLinked instanceof Map<?, ?>) {
      for (ORecord<?> rec : ((Map<String, ? extends ORecord<?>>) iLinked).values())
        sendRecord(rec);
      return true;
    } else
      throw new OFetchException("Unrecognized type while fetching records: " + iLinked);
  }
}
