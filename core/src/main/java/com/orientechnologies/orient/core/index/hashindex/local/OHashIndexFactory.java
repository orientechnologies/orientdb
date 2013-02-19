package com.orientechnologies.orient.core.index.hashindex.local;

import java.util.Collections;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexInternal;

/**
 * 
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OHashIndexFactory implements OIndexFactory {
  public static final Set<String> SUPPORTED_TYPES = Collections.singleton(OIndexUnique.TYPE_ID);

  @Override
  public Set<String> getTypes() {
    return SUPPORTED_TYPES;
  }

  @Override
  public OIndexInternal<?> createIndex(ODatabaseRecord iDatabase, String iIndexType) throws OConfigurationException {
    if (OIndexUnique.TYPE_ID.equals(iIndexType)) {
      return new OIndexUnique();
    }

    throw new OConfigurationException("Unsupported type : " + iIndexType);
  }
}
