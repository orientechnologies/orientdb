package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.HashMap;
import java.util.Map;

public abstract class OViewImpl extends OClassImpl implements OView {

  private OViewConfig cfg;

  protected OViewImpl(OSchemaShared iOwner, String iName, OViewConfig cfg, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
    this.cfg = cfg.copy();
  }

  protected OViewImpl(OSchemaShared iOwner, ODocument iDocument, String iName) {
    super(iOwner, iDocument, iName);
  }

  @Override
  public void fromStream() {
    super.fromStream();
    String query = document.getProperty("query");
    this.cfg = new OViewConfig(getName(), query);
    this.cfg.setUpdatable(Boolean.TRUE.equals(document.getProperty("updatable")));
    Map<String, Map<String, String>> idxData = document.getProperty("indexes");
    for (Map.Entry<String, Map<String, String>> idx : idxData.entrySet()) {
      OViewConfig.OViewIndexConfig indexConfig = this.cfg.addIndex(idx.getKey());
      for (Map.Entry<String, String> prop : idx.getValue().entrySet()) {
        indexConfig.addProperty(prop.getKey(), OType.valueOf(prop.getValue()));
      }
    }
  }

  @Override
  public ODocument toStream() {
    ODocument result = super.toStream();
    result.setProperty("query", cfg.getQuery());
    result.setProperty("updatable", cfg.isUpdatable());
    Map<String, Map<String, String>> indexes = new HashMap<>();
    for (OViewConfig.OViewIndexConfig idx : cfg.indexes) {
      Map<String, String> indexDescriptor = new HashMap<>();
      for (OPair<String, OType> s : idx.props) {
        indexDescriptor.put(s.key, s.value.toString());
      }
      indexes.put(idx.name, indexDescriptor);
    }
    result.setProperty("indexes", indexes);
    return result;
  }

  @Override
  public ODocument toNetworkStream() {
    ODocument result = super.toNetworkStream();
    result.setProperty("query", cfg.getQuery());
    result.setProperty("updatable", cfg.isUpdatable());
    Map<String, Map<String, String>> indexes = new HashMap<>();
    for (OViewConfig.OViewIndexConfig idx : cfg.indexes) {
      Map<String, String> indexDescriptor = new HashMap<>();
      for (OPair<String, OType> s : idx.props) {
        indexDescriptor.put(s.key, s.value.toString());
      }
      indexes.put(idx.name, indexDescriptor);
    }
    result.setProperty("indexes", indexes);
    return result;
  }

  @Override
  public String getQuery() {
    return cfg.getQuery();
  }

}
