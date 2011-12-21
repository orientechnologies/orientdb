package com.orientechnologies.orient.test.domain.whiz;

import javax.persistence.Embedded;
import java.util.Collection;
import java.util.Map;
import com.orientechnologies.orient.core.annotation.OId;

/**
 * @author LomakiA <a href="mailto:lomakin.andrey@gamil.com">Andrey Lomakin</a>
 * @since 21.12.11
 */
public class Mapper
{
  @OId
  private String							id;

  @Embedded
  private Map<String, Integer> intMap;

  public String getId()
  {
    return id;
  }

  public void setId( final String id )
  {
    this.id = id;
  }

  public Map<String, Integer> getIntMap()
  {
    return intMap;
  }

  public void setIntMap( final Map<String, Integer> intMap )
  {
    this.intMap = intMap;
  }
}
