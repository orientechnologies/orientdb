package com.orientechnologies.orient.test.domain.whiz;

import com.orientechnologies.orient.core.annotation.OId;
import com.orientechnologies.orient.core.annotation.OVersion;
import java.util.Map;
import javax.persistence.Embedded;

/**
 * @author LomakiA <a href="mailto:lomakin.andrey@gamil.com">Andrey Lomakin</a>
 * @since 21.12.11
 */
public class Mapper {
  @OId private String id;

  @OVersion private String version;

  @Embedded private Map<String, Integer> intMap;

  public String getId() {
    return id;
  }

  public void setId(final String id) {
    this.id = id;
  }

  public Map<String, Integer> getIntMap() {
    return intMap;
  }

  public void setIntMap(final Map<String, Integer> intMap) {
    this.intMap = intMap;
  }
}
