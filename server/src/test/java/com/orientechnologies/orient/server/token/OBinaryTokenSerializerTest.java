package com.orientechnologies.orient.server.token;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;


import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.binary.impl.OBinaryToken;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OBinaryTokenSerializerTest {

  private OBinaryTokenSerializer ser = new OBinaryTokenSerializer(new String[] { "plocal", "memory" }, new String[] { "key" },
                                         new String[] { "HmacSHA256" }, new String[] { "OrientDB" });

  @Test
  public void testSerializerDeserializeToken() throws IOException {
    OBinaryToken token = new OBinaryToken();
    token.setDatabase("test");
    token.setDatabaseType("plocal");
    token.setUserRid(new ORecordId(43, 234));
    OrientJwtHeader header = new OrientJwtHeader();
    header.setKeyId("key");
    header.setAlgorithm("HmacSHA256");
    header.setType("OrientDB");
    token.setHeader(header);
    token.setExpiry(20L);
    token.setProtocolVersion((short) 2);
    token.setSerializer("ser");
    token.setDriverName("aa");
    token.setDriverVersion("aa");
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    ser.serialize(token, bas);
    ByteArrayInputStream input = new ByteArrayInputStream(bas.toByteArray());
    OBinaryToken tok = ser.deserialize(input);

    assertEquals("test", token.getDatabase());
    assertEquals("plocal", token.getDatabaseType());
    ORID id = token.getUserId();
    assertEquals(43, id.getClusterId());
    assertEquals(20L, tok.getExpiry());

    assertEquals("OrientDB", tok.getHeader().getType());
    assertEquals("HmacSHA256", tok.getHeader().getAlgorithm());
    assertEquals("key", tok.getHeader().getKeyId());

    assertEquals((short) 2, tok.getProtocolVersion());
    assertEquals("ser", tok.getSerializer());
    assertEquals("aa", tok.getDriverName());
    assertEquals("aa", tok.getDriverVersion());

  }

  @Test
  public void testSerializerDeserializeServerUserToken() throws IOException {
    OBinaryToken token = new OBinaryToken();
    token.setDatabase("test");
    token.setDatabaseType("plocal");
    token.setUserRid(new ORecordId(43, 234));
    OrientJwtHeader header = new OrientJwtHeader();
    header.setKeyId("key");
    header.setAlgorithm("HmacSHA256");
    header.setType("OrientDB");
    token.setHeader(header);
    token.setExpiry(20L);
    token.setServerUser(true);
    token.setUserName("aaa");
    token.setProtocolVersion((short) 2);
    token.setSerializer("ser");
    token.setDriverName("aa");
    token.setDriverVersion("aa");
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    ser.serialize(token, bas);
    ByteArrayInputStream input = new ByteArrayInputStream(bas.toByteArray());
    OBinaryToken tok = ser.deserialize(input);

    assertEquals("test", token.getDatabase());
    assertEquals("plocal", token.getDatabaseType());
    ORID id = token.getUserId();
    assertEquals(43, id.getClusterId());
    assertEquals(20L, tok.getExpiry());
    assertTrue(token.isServerUser());
    assertEquals("aaa", tok.getUserName());

    assertEquals("OrientDB", tok.getHeader().getType());
    assertEquals("HmacSHA256", tok.getHeader().getAlgorithm());
    assertEquals("key", tok.getHeader().getKeyId());

    assertEquals((short) 2, tok.getProtocolVersion());
    assertEquals("ser", tok.getSerializer());
    assertEquals("aa", tok.getDriverName());
    assertEquals("aa", tok.getDriverVersion());
  }

  @Test
  public void testSerializerDeserializeNullInfoUserToken() throws IOException {
    OBinaryToken token = new OBinaryToken();
    token.setDatabase(null);
    token.setDatabaseType(null);
    token.setUserRid(null);
    OrientJwtHeader header = new OrientJwtHeader();
    header.setKeyId("key");
    header.setAlgorithm("HmacSHA256");
    header.setType("OrientDB");
    token.setHeader(header);
    token.setExpiry(20L);
    token.setServerUser(true);
    token.setUserName("aaa");
    token.setProtocolVersion((short) 2);
    token.setSerializer("ser");
    token.setDriverName("aa");
    token.setDriverVersion("aa");
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    ser.serialize(token, bas);
    ByteArrayInputStream input = new ByteArrayInputStream(bas.toByteArray());
    OBinaryToken tok = ser.deserialize(input);

    assertNull(token.getDatabase());
    assertNull(token.getDatabaseType());
    ORID id = token.getUserId();
    assertNull(id);
    assertEquals(20L, tok.getExpiry());
    assertTrue(token.isServerUser());
    assertEquals("aaa", tok.getUserName());

    assertEquals("OrientDB", tok.getHeader().getType());
    assertEquals("HmacSHA256", tok.getHeader().getAlgorithm());
    assertEquals("key", tok.getHeader().getKeyId());

    assertEquals((short) 2, tok.getProtocolVersion());
    assertEquals("ser", tok.getSerializer());
    assertEquals("aa", tok.getDriverName());
    assertEquals("aa", tok.getDriverVersion());

  }

}
