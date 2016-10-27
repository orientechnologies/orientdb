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

import java.util.Random;
import java.util.concurrent.Callable;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OApi;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public abstract class OSequence {
  public static final long       DEFAULT_START     = 0;
  public static final int        DEFAULT_INCREMENT = 1;
  public static final int        DEFAULT_CACHE     = 20;

  protected static final int     DEF_MAX_RETRY     = OGlobalConfiguration.SEQUENCE_MAX_RETRY.getValueAsInteger();
  public static final String     CLASS_NAME        = "OSequence";

  private static final String    FIELD_START       = "start";
  private static final String    FIELD_INCREMENT   = "incr";
  private static final String    FIELD_VALUE       = "value";

  private static final String    FIELD_NAME        = "name";
  private static final String    FIELD_TYPE        = "type";

  private ODocument              document;
  private ThreadLocal<ODocument> tlDocument        = new ThreadLocal<ODocument>();

  public static class CreateParams {
    public Long    start     = DEFAULT_START;
    public Integer increment = DEFAULT_INCREMENT;
    public Integer cacheSize = DEFAULT_CACHE;

    public CreateParams setStart(Long start) {
      this.start = start;
      return this;
    }

    public CreateParams setIncrement(Integer increment) {
      this.increment = increment;
      return this;
    }

    public CreateParams setCacheSize(Integer cacheSize) {
      this.cacheSize = cacheSize;
      return this;
    }

    public CreateParams() {
    }

    public CreateParams setDefaults() {
      this.start = this.start != null ? this.start : DEFAULT_START;
      this.increment = this.increment != null ? this.increment : DEFAULT_INCREMENT;
      this.cacheSize = this.cacheSize != null ? this.cacheSize : DEFAULT_CACHE;

      return this;
    }
  }

  public enum SEQUENCE_TYPE {
    CACHED, ORDERED,;
  }

  private int maxRetry = DEF_MAX_RETRY;

  protected OSequence() {
    this(null, null);
  }

  protected OSequence(final ODocument iDocument) {
    this(iDocument, null);
  }

  protected OSequence(final ODocument iDocument, CreateParams params) {
    document = iDocument != null ? iDocument : new ODocument(CLASS_NAME);
    bindOnLocalThread();

    if (iDocument == null) {
      if (params == null) {
        params = new CreateParams().setDefaults();
      }

      initSequence(params);

      document = getDocument();
    }
  }

  public void save() {
    tlDocument.get().save();
  }

  void bindOnLocalThread() {
    tlDocument.set(document.copy());
  }

  public ODocument getDocument() {
    return tlDocument.get();
  }

  protected synchronized void initSequence(OSequence.CreateParams params) {
    setStart(params.start);
    setIncrement(params.increment);
    setValue(params.start);

    setSequenceType();
  }

  public synchronized boolean updateParams(CreateParams params) {
    boolean any = false;

    if (params.start != null && this.getStart() != params.start) {
      this.setStart(params.start);
      any = true;
    }

    if (params.increment != null && this.getIncrement() != params.increment) {
      this.setIncrement(params.increment);
      any = true;
    }

    return any;
  }

  public void onUpdate(ODocument iDocument) {
    document = iDocument;
    this.tlDocument.set(iDocument);
  }

  protected synchronized long getValue() {
    return tlDocument.get().field(FIELD_VALUE, OType.LONG);
  }

  protected synchronized void setValue(long value) {
    tlDocument.get().field(FIELD_VALUE, value);
  }

  protected synchronized int getIncrement() {
    return tlDocument.get().field(FIELD_INCREMENT, OType.INTEGER);
  }

  protected synchronized void setIncrement(int value) {
    tlDocument.get().field(FIELD_INCREMENT, value);
  }

  protected synchronized long getStart() {
    return tlDocument.get().field(FIELD_START, OType.LONG);
  }

  protected synchronized void setStart(long value) {
    tlDocument.get().field(FIELD_START, value);
  }

  public synchronized int getMaxRetry() {
    return maxRetry;
  }

  public synchronized void setMaxRetry(final int maxRetry) {
    this.maxRetry = maxRetry;
  }

  public synchronized String getName() {
    return getSequenceName(tlDocument.get());
  }

  public synchronized OSequence setName(final String name) {
    tlDocument.get().field(FIELD_NAME, name);
    return this;
  }

  private synchronized void setSequenceType() {
    tlDocument.get().field(FIELD_TYPE, getSequenceType());
  }

  protected synchronized ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  public static String getSequenceName(final ODocument iDocument) {
    return iDocument.field(FIELD_NAME, OType.STRING);
  }

  public static SEQUENCE_TYPE getSequenceType(final ODocument document) {
    String sequenceTypeStr = document.field(FIELD_TYPE);
    return SEQUENCE_TYPE.valueOf(sequenceTypeStr);
  }

  public static void initClass(OClassImpl sequenceClass) {
    sequenceClass.createProperty(OSequence.FIELD_START, OType.LONG, (OType) null, true);
    sequenceClass.createProperty(OSequence.FIELD_INCREMENT, OType.INTEGER, (OType) null, true);
    sequenceClass.createProperty(OSequence.FIELD_VALUE, OType.LONG, (OType) null, true);

    sequenceClass.createProperty(OSequence.FIELD_NAME, OType.STRING, (OType) null, true);
    sequenceClass.createProperty(OSequence.FIELD_TYPE, OType.STRING, (OType) null, true);
  }

  /*
   * Forwards the sequence by one, and returns the new value.
   */
  @OApi
  public abstract long next();

  /*
   * Returns the current sequence value. If next() was never called, returns null
   */
  @OApi
  public abstract long current();

  /*
   * Resets the sequence value to it's initialized value.
   */
  @OApi
  public abstract long reset();

  /*
   * Returns the sequence type
   */
  public abstract SEQUENCE_TYPE getSequenceType();

  protected void checkForUpdateToLastversion() {
    final ODocument tlDoc = tlDocument.get();
    if (tlDoc != null) {
      if (document.getVersion() > tlDoc.getVersion())
        tlDocument.set(document);
    }
  }

  protected void reloadSequence() {
    tlDocument.set(tlDocument.get().reload(null, true));
  }

  protected <T> T callRetry(final Callable<T> callable, final String method) {
    for (int retry = 0; retry < maxRetry; ++retry) {
      try {
        return callable.call();
      } catch (OConcurrentModificationException ex) {
        try {
          Thread.sleep(1 + new Random().nextInt(OGlobalConfiguration.SEQUENCE_RETRY_DELAY.getValueAsInteger()));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        reloadSequence();
      } catch (OStorageException e) {
        if (e.getCause() instanceof OConcurrentModificationException) {
          reloadSequence();
        } else {
          throw OException
              .wrapException(new OSequenceException("Error in transactional processing of " + getName() + "." + method + "()"), e);
        }
      } catch (OException ex) {
        reloadSequence();
      } catch (Exception e) {
        throw OException
            .wrapException(new OSequenceException("Error in transactional processing of " + getName() + "." + method + "()"), e);
      }
    }

    try {
      return callable.call();
    } catch (Exception e) {
      if (e.getCause() instanceof OConcurrentModificationException) {
        throw ((OConcurrentModificationException) e.getCause());
      }
      throw OException
          .wrapException(new OSequenceException("Error in transactional processing of " + getName() + "." + method + "()"), e);
    }
  }
}
