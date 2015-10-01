package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OApi;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

import java.util.concurrent.Callable;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public abstract class OSequence extends ODocumentWrapper {
  public static final long    DEFAULT_START     = 0;
  public static final int     DEFAULT_INCREMENT = 1;
  public static final int     DEFAULT_CACHE     = 20;

  protected static final int  RETRY_COUNT       = 100;
  public static final String  CLASS_NAME        = "OSequence";

  private static final String FIELD_START       = "start";
  private static final String FIELD_INCREMENT   = "incr";
  private static final String FIELD_VALUE       = "value";

  private static final String FIELD_NAME        = "name";
  private static final String FIELD_TYPE        = "type";

  public static class CreateParams {
    public Long    start;
    public Integer increment;
    public Integer cacheSize;

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
    CACHED, ORDERED, ;
  }

  protected OSequence() {
    this(null, null);
  }

  protected OSequence(final ODocument iDocument) {
    this(iDocument, null);
  }

  protected OSequence(final ODocument iDocument, CreateParams params) {
    super(iDocument != null ? iDocument : new ODocument(CLASS_NAME));

    if (iDocument == null) {
      if (params == null) {
        params = new CreateParams().setDefaults();
      }

      initSequence(params);
    }
  }

  protected void initSequence(OSequence.CreateParams params) {
    setStart(params.start);
    setIncrement(params.increment);
    setValue(params.start);

    setSequenceType();
  }

  public boolean updateParams(CreateParams params) {
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
    this.document = iDocument;
  }

  protected long getValue() {
    return document.field(FIELD_VALUE, OType.LONG);
  }

  protected void setValue(long value) {
    document.field(FIELD_VALUE, value);
  }

  protected int getIncrement() {
    return document.field(FIELD_INCREMENT, OType.INTEGER);
  }

  protected void setIncrement(int value) {
    document.field(FIELD_INCREMENT, value);
  }

  protected long getStart() {
    return document.field(FIELD_START, OType.LONG);
  }

  protected void setStart(long value) {
    document.field(FIELD_START, value);
  }

  public String getName() {
    return getSequenceName(document);
  }

  public OSequence setName(final String name) {
    document.field(FIELD_NAME, name);
    return this;
  }

  private void setSequenceType() {
    document.field(FIELD_TYPE, getSequenceType());
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  public static String getSequenceName(final ODocument iDocument) {
    return iDocument.field(FIELD_NAME, OType.STRING);
  }

  public static SEQUENCE_TYPE getSequenceType(final ODocument document) {
    String sequenceTypeStr = document.field(FIELD_TYPE);
    return SEQUENCE_TYPE.valueOf(sequenceTypeStr);
  }

  public static void initClass(OClass sequenceClass) {
    sequenceClass.createProperty(OSequence.FIELD_START, OType.LONG);
    sequenceClass.createProperty(OSequence.FIELD_INCREMENT, OType.INTEGER);
    sequenceClass.createProperty(OSequence.FIELD_VALUE, OType.LONG);

    sequenceClass.createProperty(OSequence.FIELD_NAME, OType.STRING);
    sequenceClass.createProperty(OSequence.FIELD_TYPE, OType.STRING);
  }

  protected void reloadSequence() {
    reload(null, true);
  }

  private <T> T callInTx(Callable<T> callable) throws Exception {
    ODatabaseDocumentInternal database = getDatabase();
    boolean startTx = !database.getTransaction().isActive();
    if (startTx) {
      database.begin();
    }
    try {
      return callable.call();
    } finally {
      if (startTx) {
        database.commit();
      }
    }
  }

  protected <T> T callRetry(Callable<T> callable) {
    int retry = RETRY_COUNT;
    while (retry > 0) {
      try {
        return callInTx(callable);
      } catch (OConcurrentModificationException ex) {
        --retry;
        reloadSequence();
      } catch (OStorageException e) {
        if (e.getCause() instanceof OConcurrentModificationException) {
          --retry;
          reloadSequence();
        } else {
          throw OException.wrapException(new OSequenceException("Error in transaction processing of sequence method"), e);
        }
      } catch (OException ex) {
        --retry;
        reloadSequence();
      } catch (Exception e) {
        throw OException.wrapException(new OSequenceException("Error in transaction processing of sequence method"), e);
      }
    }
    try {
      return callInTx(callable);
    } catch (Exception e) {
      if (e.getCause() instanceof OConcurrentModificationException) {
        throw ((OConcurrentModificationException) e.getCause());
      }
      throw OException.wrapException(new OSequenceException("Error in transaction processing of sequence method"), e);
    }
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
}
