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
  public static final Set<String> SUPPORTED_TYPES = Collections.singleton(OUniqueHashIndex.TYPE_ID);

  @Override
  public Set<String> getTypes() {
    return SUPPORTED_TYPES;
  }

  @Override
  public OIndexInternal<?> createIndex(ODatabaseRecord iDatabase, String iIndexType) throws OConfigurationException {
    if (OUniqueHashIndex.TYPE_ID.equals(iIndexType)) {
      return new OUniqueHashIndex();
    }

    throw new OConfigurationException("Unsupported type : " + iIndexType);
  }
}
