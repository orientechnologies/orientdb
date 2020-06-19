package org.apache.tinkerpop.gremlin.orientdb;

import static org.apache.tinkerpop.gremlin.orientdb.OrientGraphUtils.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class OrientGraphUtilsTest {

  @Test
  public void encode() {
    assertThat(encodeClassName(null), nullValue());
    assertThat(encodeClassName("01"), equalTo("-01"));
    assertThat(encodeClassName("my spaced url"), equalTo("my+spaced+url"));
    assertThat(encodeClassName("UnChAnGeD"), equalTo("UnChAnGeD"));
  }

  @Test
  public void decode() {
    assertThat(decodeClassName(null), nullValue());
    assertThat(decodeClassName("-01"), equalTo("01"));
    assertThat(decodeClassName("my+spaced+url"), equalTo("my spaced url"));
    assertThat(decodeClassName("UnChAnGeD"), equalTo("UnChAnGeD"));
  }
}
