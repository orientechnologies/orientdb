/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordWrapperAbstract;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

@SuppressWarnings("unchecked")
public class ODatabaseDocumentTx extends ODatabaseRecordWrapperAbstract<ODatabaseRecordTx> implements ODatabaseDocument {
	public ODatabaseDocumentTx(final String iURL) {
		super(new ODatabaseRecordTx(iURL, ODocument.RECORD_TYPE));
	}

	@Override
	public ODocument newInstance() {
		return new ODocument(this);
	}

	public ODocument newInstance(final String iClassName) {
		checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, iClassName);
		return new ODocument(this, iClassName);
	}

	public ORecordIteratorClass<ODocument> browseClass(final String iClassName) {
		return browseClass(iClassName, true);
	}

	public ORecordIteratorClass<ODocument> browseClass(final String iClassName, final boolean iPolymorphic) {
		if (getMetadata().getSchema().getClass(iClassName) == null)
			throw new IllegalArgumentException("Class '" + iClassName + "' not found in current database");

		checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, iClassName);

		return new ORecordIteratorClass<ODocument>(this, underlying, iClassName, iPolymorphic);
	}

	@Override
	public ORecordIteratorCluster<ODocument> browseCluster(final String iClusterName) {
		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);

		return new ORecordIteratorCluster<ODocument>(this, underlying, getClusterIdByName(iClusterName));
	}

	/**
	 * Saves a document to the database. Behavior depends by the current running transaction if any. If no transaction is running then
	 * changes apply immediately. If an Optimistic transaction is running then the record will be changed at commit time. The current
	 * transaction will continue to see the record as modified, while others not. If a Pessimistic transaction is running, then an
	 * exclusive lock is acquired against the record. Current transaction will continue to see the record as modified, while others
	 * can't access to it since it's locked.
	 * 
	 * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
	 * {@link OConcurrentModificationException} exception is thrown.Before to save the document it must be valid following the
	 * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
	 * {@link ODocument#validate()} is called.
	 * 
	 * @param iRecord
	 *          Record to save.
	 * @see #setMVCC(boolean), {@link #isMVCC()}
	 * @throws OConcurrentModificationException
	 *           if the version of the document is different by the version contained in the database.
	 * @throws OValidationException
	 *           if the document breaks some validation constraints defined in the schema
	 * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
	 */
	@Override
	public ODatabaseDocumentTx save(final ORecordInternal<?> iRecord) {
		if (!(iRecord instanceof ODocument))
			return (ODatabaseDocumentTx) super.save(iRecord);

		final ODocument doc = (ODocument) iRecord;
		doc.validate();

		try {
			if (doc.getIdentity().isNew()) {
				// NEW RECORD
				if (doc.getClassName() != null)
					checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, doc.getClassName());

				if (doc.getSchemaClass() != null) {
					// CLASS FOUND: FORCE THE STORING IN THE CLUSTER CONFIGURED
					String clusterName = getClusterNameById(doc.getSchemaClass().getDefaultClusterId());

					super.save(doc, clusterName);
					return this;
				}
			} else {
				// UPDATE: CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
				if (doc.getClassName() != null)
					checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_UPDATE, doc.getClassName());
			}

			super.save(doc);

		} catch (OException e) {
			// PASS THROUGH
			throw e;
		} catch (Exception e) {
			OLogManager.instance().exception("Error on saving record %s of class '%s'", e, ODatabaseException.class,
					iRecord.getIdentity(), (doc.getClassName() != null ? doc.getClassName() : "?"));
		}
		return this;
	}

	/**
	 * Saves a document specifying a cluster where to store the record. Behavior depends by the current running transaction if any. If
	 * no transaction is running then changes apply immediately. If an Optimistic transaction is running then the record will be
	 * changed at commit time. The current transaction will continue to see the record as modified, while others not. If a Pessimistic
	 * transaction is running, then an exclusive lock is acquired against the record. Current transaction will continue to see the
	 * record as modified, while others can't access to it since it's locked.
	 * 
	 * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
	 * {@link OConcurrentModificationException} exception is thrown. Before to save the document it must be valid following the
	 * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
	 * {@link ODocument#validate()} is called.
	 * 
	 * @param iRecord
	 *          Record to save
	 * @param iClusterName
	 *          Cluster name where to save the record
	 * @see #setMVCC(boolean), {@link #isMVCC()}, ORecordSchemaAware#validate()
	 * @throws OConcurrentModificationException
	 *           if the version of the document is different by the version contained in the database.
	 * @throws OValidationException
	 *           if the document breaks some validation constraints defined in the schema
	 * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
	 */
	@Override
	public ODatabaseDocumentTx save(final ORecordInternal<?> iContent, String iClusterName) {
		if (!(iContent instanceof ODocument))
			return (ODatabaseDocumentTx) super.save(iContent, iClusterName);

		final ODocument doc = (ODocument) iContent;

		if (!doc.getIdentity().isValid()) {
			if (doc.getClassName() != null)
				checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, doc.getClassName());

			if (iClusterName == null && doc.getSchemaClass() != null)
				// FIND THE RIGHT CLUSTER AS CONFIGURED IN CLASS
				iClusterName = getClusterNameById(doc.getSchemaClass().getDefaultClusterId());

			int id = getClusterIdByName(iClusterName);
			if (id == -1)
				throw new IllegalArgumentException("Cluster name " + iClusterName + " is not configured");

			final int[] clusterIds;
			if (doc.getSchemaClass() != null) {
				// throw new IllegalArgumentException("Class '" + iClusterName + "' not configured in the record to save");

				// CHECK IF THE CLUSTER IS PART OF THE CONFIGURED CLUSTERS
				clusterIds = doc.getSchemaClass().getClusterIds();
				int i = 0;
				for (; i < clusterIds.length; ++i)
					if (clusterIds[i] == id)
						break;
			} else
				clusterIds = new int[] { id };

			if (id == clusterIds.length)
				throw new IllegalArgumentException("Cluster name " + iClusterName + " is not configured to store the class "
						+ doc.getClassName());
		} else {
			// UPDATE: CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
			if (doc.getClassName() != null)
				checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_UPDATE, doc.getClassName());
		}

		doc.validate();

		super.save(doc, iClusterName);
		return this;
	}

	/**
	 * Deletes a document. Behavior depends by the current running transaction if any. If no transaction is running then the record is
	 * deleted immediately. If an Optimistic transaction is running then the record will be deleted at commit time. The current
	 * transaction will continue to see the record as deleted, while others not. If a Pessimistic transaction is running, then an
	 * exclusive lock is acquired against the record. Current transaction will continue to see the record as deleted, while others
	 * can't access to it since it's locked.
	 * 
	 * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
	 * {@link OConcurrentModificationException} exception is thrown.
	 * 
	 * @param iContent
	 * @see #setMVCC(boolean), {@link #isMVCC()}
	 * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
	 */
	public ODatabaseDocumentTx delete(final ODocument iContent) {
		// CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
		if (iContent.getClassName() != null)
			checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_DELETE, iContent.getClassName());

		try {
			underlying.delete(iContent);

			// ODocumentHelper.deleteCrossRefs(iContent.getIdentity(), iContent);

		} catch (Exception e) {
			OLogManager.instance().exception("Error on deleting record %s of class '%s'", e, ODatabaseException.class,
					iContent.getIdentity(), iContent.getClassName());
		}
		return this;
	}

	/**
	 * Returns the number of the records of the class iClassName.
	 */
	public long countClass(final String iClassName) {
		final OClass cls = getMetadata().getSchema().getClass(iClassName);

		if (cls == null)
			throw new IllegalArgumentException("Class '" + iClassName + "' not found in database");

		return cls.count();
	}

	public ODatabaseComplex<ORecordInternal<?>> commit() {
		try {
			return underlying.commit();
		} finally {
			getTransaction().close();
		}
	}

	public ODatabaseComplex<ORecordInternal<?>> rollback() {
		try {
			return underlying.rollback();
		} finally {
			getTransaction().close();
		}
	}
}
