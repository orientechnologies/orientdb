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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.concurrent.Callable;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @see OSequenceCached
 * @since 2/28/2015
 *     <p>A sequence with sequential guarantees. Even when a transaction is rolled back, there will
 *     still be no holes. However, as a result, it is slower.
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
  public long nextWork() throws OSequenceLimitReachedException {
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
        return callRetry(
            true,
            new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                long newValue;
                Long limitVlaue = getLimitValue();
                if (getOrderType() == SequenceOrderType.ORDER_POSITIVE) {
                  newValue = getValue() + getIncrement();
                  if (limitVlaue != null && newValue > limitVlaue) {
                    if (getRecyclable()) {
                      newValue = getStart();
                    } else {
                      throw new OSequenceLimitReachedException("Limit reached");
                    }
                  }
                } else {
                  newValue = getValue() - getIncrement();
                  if (limitVlaue != null && newValue < limitVlaue) {
                    if (getRecyclable()) {
                      newValue = getStart();
                    } else {
                      throw new OSequenceLimitReachedException("Limit reached");
                    }
                  }
                }

                setValue(newValue);

                save(finalDb);

                Long limitValue = getLimitValue();
                if (limitValue != null && !getRecyclable()) {
                  float increment = getIncrement();
                  float tillEnd = Math.abs(limitValue - newValue) / increment;
                  float delta = Math.abs(limitValue - getStart()) / increment;
                  // warning on 1%
                  if ((float) tillEnd <= ((float) delta / 100.f) || tillEnd <= 1) {
                    String warningMessage =
                        "Non-recyclable sequence: "
                            + getName()
                            + " reaching limt, current value: "
                            + newValue
                            + " limit value: "
                            + limitValue
                            + " with step: "
                            + increment;
                    OLogManager.instance().warn(this, warningMessage);
                  }
                }

                return newValue;
              }
            },
            "next");
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
  protected synchronized long currentWork() {
    return callRetry(
        true,
        new Callable<Long>() {
          @Override
          public Long call() throws Exception {
            return getValue();
          }
        },
        "current");
  }

  @Override
  public synchronized long resetWork() {
    return callRetry(
        true,
        new Callable<Long>() {
          @Override
          public Long call() throws Exception {
            long newValue = getStart();
            setValue(newValue);
            save(getDatabase());

            return newValue;
          }
        },
        "reset");
  }

  @Override
  public SEQUENCE_TYPE getSequenceType() {
    return SEQUENCE_TYPE.ORDERED;
  }
}
