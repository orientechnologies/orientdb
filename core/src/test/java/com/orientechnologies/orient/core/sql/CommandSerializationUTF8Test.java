package com.orientechnologies.orient.core.sql;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class CommandSerializationUTF8Test {

  @Test
  public void testRightSerializationEncoding() {

    OSQLQuery<?> query = new OSQLSynchQuery<Object>("select from Profile where name='😢😂 '");

    Assert.assertEquals(query.toStream().length, 66);

    OSQLQuery<?> query1 = new OSQLSynchQuery<Object>();
    query1.fromStream(query.toStream());

    Assert.assertEquals(query1.getText(), "select from Profile where name='😢😂 '");

  }

}
