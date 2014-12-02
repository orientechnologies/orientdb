package com.orientechnologies.common.console;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class ODFACommandStreamTest {
  @Test
  public void testNextCommand() throws Exception {
    test("one;two", "one", "two");
  }

  @Test
  public void testNextCommandQuotes() throws Exception {
    test("Select 'one;'; Select \"t;w;o\"", "Select 'one;'", "Select \"t;w;o\"");
  }

  @Test
  public void testNextCommandSeparatorAtTheEnd() throws Exception {
    test("one;two;", "one", "two");
  }

  @Test
  public void testNextCommandWhitespaces() throws Exception {
    test("\tone  ; two   ", "one", "two");
  }

	@Test
  public void testEscaping() {
			test("one \\\n two \\\r 'aaa \\' bbb';  \"ccc \\\" \"", "one \\\n two \\\r 'aaa \\' bbb'", "\"ccc \\\" \"");
  }

  private void test(String source, String... expectedResults) {
    final ODFACommandStream stream = new ODFACommandStream(source);

    for (String expectedResult : expectedResults) {
      Assert.assertTrue(stream.hasNext());

      String result = stream.nextCommand();
      Assert.assertEquals(result, expectedResult);
    }

    Assert.assertFalse(stream.hasNext());
  }
}
