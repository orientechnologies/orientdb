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
  public long next() {
    return callRetry(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        long newValue = getValue() + getIncrement();
        setValue(newValue);

        save();

        return newValue;
      }
    });
  }

  @Override
  public long current() {
    return getValue();
  }

  @Override
  public long reset() {
    return callRetry(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        long newValue = getStart();
        setValue(newValue);
        save();

        return newValue;
      }
    });
  }

  @Override
  public SEQUENCE_TYPE getSequenceType() {
    return SEQUENCE_TYPE.ORDERED;
  }
}