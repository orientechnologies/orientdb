package com.orientechnologies.orient.test.domain.base;

import java.util.Map;
import javax.persistence.Id;
import javax.persistence.Version;

public class SimpleObject {

  @Id private Object rid;

  @Version private Object version;

  private String objectId;

  private Map<String, String> templatePartsIds;

  public String getObjectId() {
    return objectId;
  }

  public void setObjectId(String objectId) {
    this.objectId = objectId;
  }

  public Map<String, String> getTemplatePartsIds() {
    return templatePartsIds;
  }

  public void setTemplatePartsIds(Map<String, String> templatePartsIds) {
    this.templatePartsIds = templatePartsIds;
  }

  public Object getRid() {
    return rid;
  }
}
