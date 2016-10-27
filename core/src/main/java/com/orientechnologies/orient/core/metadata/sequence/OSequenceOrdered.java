/*
 * Copyright 2010-2014 OrientDB LTD (info--at--orientdb.com)
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
package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.concurrent.Callable;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 2/28/2015
 *
 * A sequence with sequential guarantees. Even when a transaction is rolled back,
 * there will still be no holes. However, as a result, it is slower.
 * @see OSequenceCached
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
    return callRetry(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        long newValue = getValue() + getIncrement();
        setValue(newValue);

        save();

        return newValue;
      }
    }, "next");
  }

  @Override
  public synchronized long current() {
    return getValue();
  }

  @Override
  public synchronized long reset() {
    return callRetry(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        long newValue = getStart();
        setValue(newValue);
        save();

        return newValue;
      }
    }, "reset");
  }

  @Override
  public SEQUENCE_TYPE getSequenceType() {
    return SEQUENCE_TYPE.ORDERED;
  }
}