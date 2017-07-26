/*
 *
 *  *  Copyright 2014 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.concurrent.Callable;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @see OSequenceCached
 * @since 2/28/2015
 * <p>
 * A sequence with sequential guarantees. Even when a transaction is rolled back, there will still be no holes. However, as a
 * result, it is slower.
 */
public class OSequenceOrdered extends OSequence {
  public OSequenceOrdered() {
    super();
  }

  public OSequenceOrdered(final ODocument iDocument) {
    super(iDocument);
  }

  public OSequenceOrdered(final ODocument iDocument, OSequence.CreateParams params) {
    super(iDocument, params);
  }

  @Override
  public synchronized long next() {
    ODatabaseDocumentInternal mainDb = getDatabase();
    boolean tx = mainDb.getTransaction().isActive();
    try {
      ODatabaseDocumentInternal db = mainDb;
      if (tx) {
        db = mainDb.copy();
        db.activateOnCurrentThread();
      }
      try {
        ODatabaseDocumentInternal finalDb = db;
        return callRetry(new Callable<Long>() {
          @Override
          public Long call() throws Exception {
            long newValue = getValue() + getIncrement();
            setValue(newValue);

            save(finalDb);

            return newValue;
          }
        }, "next");
      } finally {
        if (tx) {
          db.close();
        }
      }
    } finally {
      if (tx) {
        mainDb.activateOnCurrentThread();
      }
    }
  }

  @Override
  public synchronized long current() {
    return callRetry(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return getValue();
      }
    }, "current");
  }

  @Override
  public synchronized long reset() {
    return callRetry(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        long newValue = getStart();
        setValue(newValue);
        save(getDatabase());

        return newValue;
      }
    }, "reset");
  }

  @Override
  public SEQUENCE_TYPE getSequenceType() {
    return SEQUENCE_TYPE.ORDERED;
  }
}