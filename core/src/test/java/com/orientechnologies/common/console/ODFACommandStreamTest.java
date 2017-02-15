package com.orientechnologies.common.console;

import org.junit.Assert;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class ODFACommandStreamTest {

  public void testNextCommand() throws Exception {
    test("one;two", "one", "two");
  }


  public void testNextCommandQuotes() throws Exception {
    test("Select 'one;'; Select \"t;w;o\"", "Select 'one;'", "Select \"t;w;o\"");
  }


  public void testNextCommandSeparatorAtTheEnd() throws Exception {
    test("one;two;", "one", "two");
  }


  public void testNextCommandWhitespaces() throws Exception {
    test("\tone  ; two   ", "one", "two");
  }


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
