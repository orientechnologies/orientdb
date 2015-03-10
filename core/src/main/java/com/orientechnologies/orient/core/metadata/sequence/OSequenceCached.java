package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.concurrent.Callable;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/3/2015
 */
public class OSequenceCached extends OSequence {
  private static final String FIELD_CACHE = "cache";
  private long cacheStart = 0L;
  private long cacheEnd = 0L;

  public OSequenceCached() {
    super();
  }

  public OSequenceCached(final ODocument iDocument) {
    super(iDocument);
  }

  public OSequenceCached(final ODocument iDocument, OSequence.CreateParams params) {
    super(iDocument, params);
  }

  @Override
  public boolean updateParams(OSequence.CreateParams params) {
    boolean any = super.updateParams(params);
    if (params.cacheSize != null && this.getCacheSize() != params.cacheSize) {
      this.setCacheSize(params.cacheSize);
      any = true;
    }
    return any;
  }

  @Override
  protected void initSequence(OSequence.CreateParams params) {
    super.initSequence(params);
    setCacheSize(params.cacheSize);
  }

  @Override
  public long next() {
    return callRetry(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        int increment = getIncrement();
        if (cacheStart + increment >= cacheEnd) {
          allocateCache(getCacheSize());
        }

        ++cacheStart;
        return cacheStart;
      }
    });
  }

  @Override
  public long current() {
    return this.cacheStart;
  }

  @Override
  public long reset() {
    return callRetry(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        long newValue = getStart();
        setValue(newValue);
        save();

        //
        allocateCache(getCacheSize());

        return newValue;
      }
    });
  }

  @Override
  public SEQUENCE_TYPE getSequenceType() {
    return SEQUENCE_TYPE.CACHED;
  }

  public int getCacheSize() {
    return document.field(FIELD_CACHE, OType.INTEGER);
  }

  public void setCacheSize(int cacheSize) {
    document.field(FIELD_CACHE, cacheSize);
  }

  private void allocateCache(int cacheSize) {
    long value = getValue();
    long newValue = value + (getIncrement() * cacheSize);
    setValue(newValue);
    save();

    this.cacheStart = value;
    this.cacheEnd = newValue - 1;
  }
}