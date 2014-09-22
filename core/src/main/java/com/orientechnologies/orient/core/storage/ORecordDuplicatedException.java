package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.id.ORID;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @since 9/5/12
 */
public class ORecordDuplicatedException extends OException {
  private final ORID rid;

  public ORecordDuplicatedException(final String message, final ORID iRid) {
    super(message);
    this.rid = iRid;
  }

  public ORID getRid() {
    return rid;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null || !obj.getClass().equals(getClass()))
      return false;

    return rid.equals(((ORecordDuplicatedException) obj).rid);
  }
  
  
  @Override
  public int hashCode() {
	return rid.hashCode();
  }

@Override
  public String toString() {
    return super.toString() + " RID=" + rid;
  }
}
