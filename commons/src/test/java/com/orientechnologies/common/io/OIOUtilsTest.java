package com.orientechnologies.common.io;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class OIOUtilsTest {

  @Test
  public void shouldGetTimeAsMilis() {
    assertGetTimeAsMilis("2h", 2 * 3600 * 1000);
    assertGetTimeAsMilis("500ms", 500);
    assertGetTimeAsMilis("4d", 4 * 24 * 3600 * 1000);
    assertGetTimeAsMilis("6w", 6l * 7 * 24 * 3600 * 1000);

  }

  private void assertGetTimeAsMilis(String data, long expected) {
    assertEquals(OIOUtils.getTimeAsMillisecs(data), expected);
  }

}
