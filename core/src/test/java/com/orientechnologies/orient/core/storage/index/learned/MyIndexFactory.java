package com.orientechnologies.orient.core.storage.index.learned;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MyIndexFactory implements OIndexFactory {
  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;
  private static final int VERSION = 0;

  public static final String PGM_ALGORITHM = "PGM";

  static {
    final Set<String> types = new HashSet<>();
    types.add(OClass.INDEX_TYPE.NOTUNIQUE.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<>();
    algorithms.add(PGM_ALGORITHM);
    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  @Override
  public int getLastVersion(final String algorithm) {
    return VERSION;
  }

  @Override
  public Set<String> getTypes() {
    return TYPES;
  }

  @Override
  public Set<String> getAlgorithms() {
    return ALGORITHMS;
  }

  @Override
  public OIndexInternal createIndex(
      String name,
      OStorage storage,
      String indexType,
      String algorithm,
      String valueContainerAlgorithm,
      ODocument metadata,
      int version,
      OAtomicOperationsManager atomicOperationsManager)
      throws OConfigurationException {
    return null;
  }

  @Override
  public OBaseIndexEngine createIndexEngine(
      int indexId,
      String algorithm,
      String name,
      Boolean durableInNonTxMode,
      OStorage storage,
      int version,
      int apiVersion,
      boolean multiValue,
      Map<String, String> engineProperties) {
    return null;
  }
}
