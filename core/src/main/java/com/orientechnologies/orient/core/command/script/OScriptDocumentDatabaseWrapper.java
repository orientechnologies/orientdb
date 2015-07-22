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
package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;
import com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Document Database wrapper class to use from scripts.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OScriptDocumentDatabaseWrapper {
  protected ODatabaseDocumentTx database;

  public OScriptDocumentDatabaseWrapper(final ODatabaseDocumentTx database) {
    this.database = database;
  }

  public OScriptDocumentDatabaseWrapper(final String iURL) {
    this.database = new ODatabaseDocumentTx(iURL);
  }

  public void switchUser(final String iUserName, final String iUserPassword) {
    if (!database.isClosed())
      database.close();
    database.open(iUserName, iUserPassword);
  }

  public OIdentifiable[] query(final String iText) {
    return query(iText, (Object[]) null);
  }

  public OIdentifiable[] query(final String iText, final Object... iParameters) {
    return query(new OSQLSynchQuery<Object>(iText), iParameters);
  }

  public OIdentifiable[] query(final OSQLQuery iQuery, final Object... iParameters) {
    final List<OIdentifiable> res = database.query(iQuery, convertParameters(iParameters));
    if (res == null)
      return OCommonConst.EMPTY_IDENTIFIABLE_ARRAY;
    return res.toArray(new OIdentifiable[res.size()]);
  }

  /**
   * To maintain the compatibility with JS API.
   */
  public Object executeCommand(final String iText) {
    return command(iText, (Object[]) null);
  }

  /**
   * To maintain the compatibility with JS API.
   */
  public Object executeCommand(final String iText, final Object... iParameters) {
    return command(iText, iParameters);
  }

  public Object command(final String iText) {
    return command(iText, (Object[]) null);
  }

  public Object command(final String iText, final Object... iParameters) {
    Object res = database.command(new OCommandSQL(iText)).execute(convertParameters(iParameters));
    if (res instanceof List) {
      final List<OIdentifiable> list = (List<OIdentifiable>) res;
      return list.toArray(new OIdentifiable[list.size()]);
    }
    return res;
  }

  public OIndex<?> getIndex(final String iName) {
    return database.getMetadata().getIndexManager().getIndex(iName);
  }

  public boolean exists() {
    return database.exists();
  }

  public ODocument newInstance() {
    return database.newInstance();
  }

  public void reload() {
    database.reload();
  }

  public ODocument newInstance(String iClassName) {
    return database.newInstance(iClassName);
  }

  public ORecordIteratorClass<ODocument> browseClass(String iClassName) {
    return database.browseClass(iClassName);
  }

  public STATUS getStatus() {
    return database.getStatus();
  }

  public ORecordIteratorClass<ODocument> browseClass(String iClassName, boolean iPolymorphic) {
    return database.browseClass(iClassName, iPolymorphic);
  }

  public <THISDB extends ODatabase> THISDB setStatus(STATUS iStatus) {
    return (THISDB) database.setStatus(iStatus);
  }

  public void drop() {
    database.drop();
  }

  public String getName() {
    return database.getName();
  }

  public String getURL() {
    return database.getURL();
  }

  public ORecordIteratorCluster<ODocument> browseCluster(String iClusterName) {
    return database.browseCluster(iClusterName);
  }

  public boolean isClosed() {
    return database.isClosed();
  }

  public <THISDB extends ODatabase> THISDB open(String iUserName, String iUserPassword) {
    return (THISDB) database.open(iUserName, iUserPassword);
  }

  public ODocument save(final Map<String, Object> iObject) {
    return database.save(new ODocument().fields(iObject));
  }

  public ODocument save(final String iString) {
    // return database.save((ORecord) new ODocument().fromJSON(iString));
    return database.save((ORecord) new ODocument().fromJSON(iString, true));
  }

  public ODocument save(ORecord iRecord) {
    return database.save(iRecord);
  }

  public boolean dropCluster(String iClusterName, final boolean iTruncate) {
    return database.dropCluster(iClusterName, iTruncate);
  }

  public <THISDB extends ODatabase> THISDB create() {
    return (THISDB) database.create();
  }

  public boolean dropCluster(int iClusterId, final boolean iTruncate) {
    return database.dropCluster(iClusterId, true);
  }

  public void close() {
    database.close();
  }

  public int getClusters() {
    return database.getClusters();
  }

  public Collection<String> getClusterNames() {
    return database.getClusterNames();
  }

  public OTransaction getTransaction() {
    return database.getTransaction();
  }

  public ODatabase<ORecord> begin() {
    return database.begin();
  }

  public int getClusterIdByName(String iClusterName) {
    return database.getClusterIdByName(iClusterName);
  }

  public boolean isMVCC() {
    return database.isMVCC();
  }

  public String getClusterNameById(int iClusterId) {
    return database.getClusterNameById(iClusterId);
  }

  public <RET extends ODatabase<?>> RET setMVCC(boolean iValue) {
    return (RET) database.setMVCC(iValue);
  }

  public long getClusterRecordSizeById(int iClusterId) {
    return database.getClusterRecordSizeById(iClusterId);
  }

  public boolean isValidationEnabled() {
    return database.isValidationEnabled();
  }

  public long getClusterRecordSizeByName(String iClusterName) {
    return database.getClusterRecordSizeByName(iClusterName);
  }

  public <RET extends ODatabaseDocument> RET setValidationEnabled(boolean iValue) {
    return (RET) database.setValidationEnabled(iValue);
  }

  public OSecurityUser getUser() {
    return database.getUser();
  }

  public void setUser(OUser user) {
    database.setUser(user);
  }

  public ODocument save(ORecord iRecord, OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    return database.save(iRecord, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  public OMetadata getMetadata() {
    return database.getMetadata();
  }

  public ODictionary<ORecord> getDictionary() {
    return database.getDictionary();
  }

  public byte getRecordType() {
    return database.getRecordType();
  }

  public ODatabase<ORecord> delete(ORID iRid) {
    return database.delete(iRid);
  }

  public <RET extends ORecord> RET load(ORID iRecordId) {
    return (RET) database.load(iRecordId);
  }

  public <RET extends ORecord> RET load(ORID iRecordId, String iFetchPlan) {
    return (RET) database.load(iRecordId, iFetchPlan);
  }

  public <RET extends ORecord> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache) {
    return (RET) database.load(iRecordId, iFetchPlan, iIgnoreCache);
  }

  public <RET extends ORecord> RET getRecord(OIdentifiable iIdentifiable) {
    return (RET) database.getRecord(iIdentifiable);
  }

  public int getDefaultClusterId() {
    return database.getDefaultClusterId();
  }

  public <RET extends ORecord> RET load(ORecord iRecord) {
    return (RET) database.load(iRecord);
  }

  public boolean declareIntent(OIntent iIntent) {
    return database.declareIntent(iIntent);
  }

  public <RET extends ORecord> RET load(ORecord iRecord, String iFetchPlan) {
    return (RET) database.load(iRecord, iFetchPlan);
  }

  public <RET extends ORecord> RET load(ORecord iRecord, String iFetchPlan, boolean iIgnoreCache) {
    return (RET) database.load(iRecord, iFetchPlan, iIgnoreCache);
  }

  public ODatabase<?> setDatabaseOwner(ODatabaseInternal<?> iOwner) {
    return database.setDatabaseOwner(iOwner);
  }

  public void reload(ORecord iRecord) {
    database.reload(iRecord);
  }

  public void reload(ORecord iRecord, String iFetchPlan, boolean iIgnoreCache) {
    database.reload(iRecord, iFetchPlan, iIgnoreCache);
  }

  public Object setProperty(String iName, Object iValue) {
    return database.setProperty(iName, iValue);
  }

  public ODocument save(ORecord iRecord, String iClusterName) {
    return database.save(iRecord, iClusterName);
  }

  public Object getProperty(String iName) {
    return database.getProperty(iName);
  }

  public Iterator<Entry<String, Object>> getProperties() {
    return database.getProperties();
  }

  public Object get(ATTRIBUTES iAttribute) {
    return database.get(iAttribute);
  }

  public <THISDB extends ODatabase> THISDB set(ATTRIBUTES attribute, Object iValue) {
    return (THISDB) database.set(attribute, iValue);
  }

  public void setInternal(ATTRIBUTES attribute, Object iValue) {
    database.setInternal(attribute, iValue);
  }

  public boolean isRetainRecords() {
    return database.isRetainRecords();
  }

  public ODatabaseDocument setRetainRecords(boolean iValue) {
    return database.setRetainRecords(iValue);
  }

  public long getSize() {
    return database.getSize();
  }

  public ORecord getRecordByUserObject(Object iUserObject, boolean iCreateIfNotAvailable) {
    return database.getRecordByUserObject(iUserObject, iCreateIfNotAvailable);
  }

  public ODocument save(ORecord iRecord, String iClusterName, OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    return database.save(iRecord, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  public ODatabaseDocumentTx delete(ODocument iRecord) {
    return database.delete(iRecord);
  }

  public long countClass(String iClassName) {
    return database.countClass(iClassName);
  }

  public ODatabase<ORecord> commit() {
    return database.commit();
  }

  public ODatabase<ORecord> rollback() {
    return database.rollback();
  }

  public String getType() {
    return database.getType();
  }

  protected Object[] convertParameters(final Object[] iParameters) {
    if (iParameters != null)
      for (int i = 0; i < iParameters.length; ++i) {
        final Object p = iParameters[i];
        if (p != null) {
          // if (p instanceof sun.org.mozilla.javascript.internal.IdScriptableObject) {
          // iParameters[i] = ((sun.org.mozilla.javascript.internal.NativeDate) p).to;
          // }
        }
      }
    return iParameters;
  }
}
