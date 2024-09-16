package com.orientechnologies.orient.core.sql.executor;

public class OIndexStreamStat {
  private String indexName;
  private int definitionKeySize;
  private int keySize;

  public OIndexStreamStat(String indexName, int definitionKeySize, int keySize) {
    super();
    this.indexName = indexName;
    this.definitionKeySize = definitionKeySize;
    this.keySize = keySize;
  }

  public String getIndexName() {
    return indexName;
  }

  public int getDefinitionKeySize() {
    return definitionKeySize;
  }

  public int getKeySize() {
    return keySize;
  }
}
