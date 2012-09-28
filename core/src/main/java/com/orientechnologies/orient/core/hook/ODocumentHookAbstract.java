/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.hook;

import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Hook abstract class that calls separate methods for ODocument records.
 * 
 * @author Luca Garulli
 * @see ORecordHook
 */
public abstract class ODocumentHookAbstract implements ORecordHook {
  private String[] includeClasses;
  private String[] excludeClasses;

  protected ODocumentHookAbstract() {
  }

  /**
   * It's called just before to create the new document.
   * 
   * @param iDocument
   *          The document to create
   * @return True if the document has been modified and a new marshalling is required, otherwise false
   */
  public boolean onRecordBeforeCreate(final ODocument iDocument) {
    return false;
  }

  /**
   * It's called just after the document is created.
   * 
   * @param iDocument
   *          The document is going to be created
   */
  public void onRecordAfterCreate(final ODocument iDocument) {
  }

  /**
   * It's called just after the document creation was failed.
   * 
   * @param iDocument
   *          The document just created
   */
  public void onRecordCreateFailed(final ODocument iDocument) {
  }

  /**
   * It's called just before to read the document.
   * 
   * @param iDocument
   *          The document to read
   */
  public void onRecordBeforeRead(final ODocument iDocument) {
  }

  /**
   * It's called just after the document is read.
   * 
   * @param iDocument
   *          The document just read
   */
  public void onRecordAfterRead(final ODocument iDocument) {
  }

  /**
   * It's called just before to update the document.
   * 
   * @param iDocument
   *          The document to update
   * @return True if the document has been modified and a new marshalling is required, otherwise false
   */
  public boolean onRecordBeforeUpdate(final ODocument iDocument) {
    return false;
  }

  /**
   * It's called just after the document is updated.
   * 
   * @param iDocument
   *          The document just updated
   */
  public void onRecordAfterUpdate(final ODocument iDocument) {
  }

  /**
   * It's called just after the document updated was failed.
   * 
   * @param iDocument
   *          The document is going to be updated
   */
  public void onRecordUpdateFailed(final ODocument iDocument) {
  }

  /**
   * It's called just before to delete the document.
   * 
   * @param iDocument
   *          The document to delete
   * @return True if the document has been modified and a new marshalling is required, otherwise false
   */
  public boolean onRecordBeforeDelete(final ODocument iDocument) {
    return false;
  }

  /**
   * It's called just after the document is deleted.
   * 
   * @param iDocument
   *          The document just deleted
   */
  public void onRecordAfterDelete(final ODocument iDocument) {
  }

  /**
   * It's called just after the document deletion was failed.
   * 
   * @param iDocument
   *          The document is going to be deleted
   */
  public void onRecordDeleteFailed(final ODocument iDocument) {
  }

  public boolean onTrigger(final TYPE iType, final ORecord<?> iRecord) {
    if (ODatabaseRecordThreadLocal.INSTANCE.isDefined() && ODatabaseRecordThreadLocal.INSTANCE.get().getStatus() != STATUS.OPEN)
      return false;

    if (!(iRecord instanceof ODocument))
      return false;

    final ODocument document = (ODocument) iRecord;

    if (!filterBySchemaClass(document))
      return false;

    switch (iType) {
    case BEFORE_CREATE:
      return onRecordBeforeCreate(document);

    case AFTER_CREATE:
      onRecordAfterCreate(document);
      break;

    case CREATE_FAILED:
      onRecordCreateFailed(document);
      break;

    case BEFORE_READ:
      onRecordBeforeRead(document);
      break;

    case AFTER_READ:
      onRecordAfterRead(document);
      break;

    case BEFORE_UPDATE:
      return onRecordBeforeUpdate(document);

    case AFTER_UPDATE:
      onRecordAfterUpdate(document);
      break;

    case UPDATE_FAILED:
      onRecordUpdateFailed(document);
      break;

    case BEFORE_DELETE:
      return onRecordBeforeDelete(document);

    case AFTER_DELETE:
      onRecordAfterDelete(document);
      break;

    case DELETE_FAILED:
      onRecordDeleteFailed(document);
      break;

    default:
      throw new IllegalStateException("Hook method " + iType + " is not managed");
    }

    return false;
  }

  public String[] getIncludeClasses() {
    return includeClasses;
  }

  public ODocumentHookAbstract setIncludeClasses(final String... includeClasses) {
    if (excludeClasses != null)
      throw new IllegalStateException("Cannot include classes if exclude classes has been set");
    this.includeClasses = includeClasses;
    return this;
  }

  public String[] getExcludeClasses() {
    return excludeClasses;
  }

  public ODocumentHookAbstract setExcludeClasses(final String... excludeClasses) {
    if (includeClasses != null)
      throw new IllegalStateException("Cannot exclude classes if include classes has been set");
    this.excludeClasses = excludeClasses;
    return this;
  }

  protected boolean filterBySchemaClass(final ODocument iDocument) {
    final String clazz = iDocument.getClassName();

    if (includeClasses != null) {
      if (clazz == null)
        return false;

      // FILTER BY CLASSES
      for (String cls : includeClasses)
        if (cls.equals(clazz))
          return true;
      return false;
    }

    if (excludeClasses != null) {
      if (clazz == null)
        return false;

      // FILTER BY CLASSES
      for (String cls : excludeClasses)
        if (cls.equals(clazz))
          return false;
    }
    return true;
  }
}
