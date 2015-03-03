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
package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODocumentFieldVisitor;
import com.orientechnologies.orient.core.db.document.ODocumentFieldWalker;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.version.ORecordVersion;

public class ORidBagDeleteHook extends ODocumentHookAbstract {
  
  public ORidBagDeleteHook(ODatabaseDocument database) {
   super(database);
  }
  
  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
  }

  @Override
  public RESULT onRecordBeforeDelete(ODocument document) {
    deleteAllRidBags(document);
    return RESULT.RECORD_CHANGED;
  }

  private void deleteAllRidBags(ODocument document) {
    final ORecordVersion version = document.getRecordVersion();
    if (document.fields() == 0 && document.getIdentity().isPersistent()) {
      // FORCE LOADING OF CLASS+FIELDS TO USE IT AFTER ON onRecordAfterDelete METHOD
      document.reload();
      if (version.getCounter() > -1 && document.getRecordVersion().compareTo(version) != 0) // check for record version errors
        if (OFastConcurrentModificationException.enabled())
          throw OFastConcurrentModificationException.instance();
        else
          throw new OConcurrentModificationException(document.getIdentity(), document.getRecordVersion(), version,
              ORecordOperation.DELETED);
    }

    final ODocumentFieldWalker documentFieldWalker = new ODocumentFieldWalker();
    final RidBagDeleter ridBagDeleter = new RidBagDeleter();
    documentFieldWalker.walkDocument(document, ridBagDeleter);
  }

  private static final class RidBagDeleter implements ODocumentFieldVisitor {

    @Override
    public Object visitField(OType type, OType linkedType, Object value) {
      if (value instanceof ORidBag)
        ((ORidBag) value).delete();

      return value;
    }

    @Override
    public boolean goFurther(OType type, OType linkedType, Object value, Object newValue) {
      return true;
    }

    @Override
    public boolean goDeeper(OType type, OType linkedType, Object value) {
      return true;
    }

    @Override
    public boolean updateMode() {
      return false;
    }
  }

}
