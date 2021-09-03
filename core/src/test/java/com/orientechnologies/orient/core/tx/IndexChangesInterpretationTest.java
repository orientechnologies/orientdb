/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.Interpretation;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Assert;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class IndexChangesInterpretationTest {

  // @formatter:off
  private static final String[][] TEST_VECTORS = {

    // Following domain specific language is used:
    //
    //   1. Input: 'pN' for put N into the key, 'rN' for remove N from the key, 'd' for the key
    // deletion.
    //
    //   2. Output: same as input, with addition of sets like '{N p1 p2}', there order of operation
    // doesn't matter,
    //      but N of them must match or all of them if N is not specified. Sets can't nest. Also,
    // 'r' w/o number matches
    //      any removal operation, either 'rN' or 'd'.
    //
    // "Parsers" are very ad hoc, you have been warned. As always, the only problems with DSLs is
    // that you have
    // to prove they are parsed correctly. I'm too lazy to write tests for the tests.
    //
    // 1st column is changes sequence, 2nd is expected sequence for unique index, 3rd for
    // dictionary, 4th for non-unique.

    {"", "", "", ""},
    {"p1", "p1", "p1", "p1"},
    {"r1", "r", "r", "r1"},
    {"d", "d", "d", "d"},
    {"p1 p2", "{p1 p2}", "p2", "{p1 p2}"},
    {"p1 r1", "p1 r1", "p1 r1", "p1 r1"},
    {"p1 r2", "r p1", "p1", "{p1 r2}"},
    {"r1 r2", "r", "r", "{r1 r2}"},
    {"r1 p1", "r1 p1", "r1 p1", "r1 p1"},
    // in theory, maybe optimized to an empty sequence, but it's not safe, can't touch this
    {"r2 p1", "r p1", "p1", "{p1 r2}"},
    {"r1 p1 p2", "r {p1 p2}", "p2", "{p1 p2}"},
    {"r1 p2 p1", "r {p1 p2}", "p1", "{p1 p2}"},
    {"p2 r1 p1", "r {p1 p2}", "p1", "{p1, p2}"},
    {
      "r1 r2 p2 p1", "r {p1 p2}", "p1", "{p1 p2}"
    }, // actually, invalid input, but we must support things like that
    {"r1 r2 p1 p2", "r {p1 p2}", "p2", "{p1 p2}"},
    {"r1 p1 r2 p2", "r {p1 p2}", "p2", "{p1 p2}"},
    {"p1 p2 r2", "p1", "p1", "p1"},
    {"p1 p2 r1", "p2", "p2", "p2"},
    {"p1 r1 p2", "p2", "p2", "p2"},
    {"p1 p2 p3", "{2 p1 p2 p3}", "p3", "{p1 p2 p3}"},
    {"p1 p2 p3 p4", "{2 p1 p2 p3 p4}", "p4", "{p1 p2 p3 p4}"},
    {"p1 r1", "p1 r1", "p1 r1", "p1 r1"},
    {"p1 p2 r1 r2", "", "", ""},
    {"p1 p2 r2 r1", "", "", ""},
    {"p1 r1 p2 r2", "", "", ""},
    {"p1 r1 p2 r2 r3", "r3", "r3", "r3"},
    {"p1 p1", "p1", "p1", "p1"},
    {"r1 r1", "r", "r", "r1"},
    {"p1 p1 p1", "p1", "p1", "p1"},
    {"r1 r1 r1", "r", "r", "r1"},
    {"p1 p1 p2", "{p1 p2}", "p2", "{p1 p2}"},
    {"p1 p1 p2 p1 p1", "{p1 p2}", "p1", "{p1 p2}"},
    {"r1 r1 r2 r1 r1", "r", "r", "{r1 r2}"},
    {"p1 d", "r", "r", "d"},
    {"d p1", "r p1", "p1", "d p1"},
    {"r1 d", "r", "r", "d"},
    {"d r1", "r", "r", "d"},
    {"d d", "r", "r", "d"},
    {"p1 d p1", "r p1", "p1", "d p1"},
    {"p2 d p1", "r p1", "p1", "d p1"},
    {"d p1 p2", "r {p1 p2}", "p2", "d {p1 p2}"},
    {"d p1 p2 r2", "r p1", "p1", "d p1"},
    {"r1 d r2", "r", "r", "d"},
    {"r1 d r2 d", "r", "r", "d"},
    {"d r1 r2", "r", "r", "d"},
    {"d d d", "r", "r", "d"},
    {"p1 p2 p3 p4 p5 d p1", "r p1", "p1", "d p1"},
    {"p1 p2 p3 p4 p5 d p1 p10", "r {p1 p10}", "p10", "d {p1 p10}"},
    {"p1 p2 p3 p4 p5 d p1 p10 d", "r", "r", "d"},
    {"r1 p1 p2 p3 r1 r3 d r10 p100 p200", "r {p100 p200}", "p200", "d {p100 p200}"},
    {"r1 p1 p2 p3 r1 r3 d r10 p100 p200 r100", "r p200", "p200", "d p200"},
    {"r1 p1 p2 p3 r1 r3 d r10 p100 p200 r100 d", "r", "r", "d"},
    {
      "r1 p1 p2 p3 r1 r3 d r10 p100 p200 r100 d r1 p1 p2 p3 r2 r100", "r {p1 p3}", "p3", "d {p1 p3}"
    },
    {"p1 p2 p3 r2 r3 p4", "{p1 p4}", "p4", "{p1 p4}"},
    {"p1 p2 p3 r2 p4 r3", "{p1 p4}", "p4", "{p1 p4}"},
    {"p1 p2 p3 p4 r2 r3", "{p1 p4}", "p4", "{p1 p4}"},
    {"p1 p2 p4 p3 r2 r3", "{p1 p4}", "p4", "{p1 p4}"},
    {"p1 p2 p4 p3 r3 r2", "{p1 p4}", "p4", "{p1 p4}"},
    {"p1 p4 p2 p3 r3 r2", "{p1 p4}", "p4", "{p1 p4}"},
    {"p4 p1 p2 p3 r2 r3", "{p1 p4}", "p1", "{p1 p4}"},
    {"p1 p2 p3 d p1 p2 p3", "r {2 p1 p2 p3}", "p3", "d {p1 p2 p3}"},
    {"p1 p2 p3 d p1 d p2 p3", "r {2 p2 p3}", "p3", "d {p2 p3}"},
    {"p1 p2 p3 d p1 p2 d p3", "r p3", "p3", "d p3"}
  };
  // @formatter:on

  private static final Pattern INPUT_GRAMMAR =
      Pattern.compile("\\s*([pr]\\d+|d)\\s*", Pattern.CASE_INSENSITIVE);
  private static final Pattern OUTPUT_GRAMMAR =
      Pattern.compile("\\s*([pr]\\d+|d|r|\\{.*\\})\\s*", Pattern.CASE_INSENSITIVE);
  private static final Pattern OUTPUT_ITEMS_GRAMMAR =
      Pattern.compile("\\s*([pr]\\d+|d|r)\\s*", Pattern.CASE_INSENSITIVE);

  private static String entryToString(OTransactionIndexEntry entry) {
    if (entry == null) return "r";

    return entry.getOperation() == OPERATION.PUT
        ? "p" + entry.getValue().getIdentity().getClusterPosition()
        : entry.getValue() == null
            ? "d"
            : "r" + entry.getValue().getIdentity().getClusterPosition();
  }

  private static boolean entryEquals(OTransactionIndexEntry a, OTransactionIndexEntry b) {
    if (a == b) return true;

    if (a == null) return b.getOperation() == OPERATION.REMOVE;

    if (b == null) return a.getOperation() == OPERATION.REMOVE;

    return a.getOperation() == b.getOperation() && a.equals(b);
  }

  @Test
  public void test() {
    OTransactionIndexChangesPerKey changes;

    final List<OutputCollection> expectedUnique = new ArrayList<OutputCollection>();
    final List<OutputCollection> expectedDictionary = new ArrayList<OutputCollection>();
    final List<OutputCollection> expectedNonUnique = new ArrayList<OutputCollection>();

    for (String[] vector : TEST_VECTORS) {
      changes = parseInput(vector[0]);
      parseOutput(vector[1], expectedUnique);
      parseOutput(vector[2], expectedDictionary);
      parseOutput(vector[3], expectedNonUnique);

      verify(
          expectedUnique,
          changes.interpret(Interpretation.Unique),
          "unique",
          changes.getEntriesAsList());
      verify(
          expectedDictionary,
          changes.interpret(Interpretation.Dictionary),
          "dictionary",
          changes.getEntriesAsList());
      verify(
          expectedNonUnique,
          changes.interpret(Interpretation.NonUnique),
          "non-unique",
          changes.getEntriesAsList());
    }
  }

  private OTransactionIndexChangesPerKey parseInput(String text) {
    OTransactionIndexChangesPerKey result = new OTransactionIndexChangesPerKey("key");
    final Matcher matcher = INPUT_GRAMMAR.matcher(text);
    while (matcher.find()) {
      // TODO this is a hack! The logic should go through OTransactionIndexChangesPerKey.add(),
      // not create the entries manually
      OTransactionIndexEntry change = parseChange(matcher.group(1));
      result
          .getEntriesInternal()
          .add(result.createEntryInternal(change.getValue(), change.getOperation()));
    }
    return result;
  }

  private void parseOutputItems(String text, Collection<OTransactionIndexEntry> result) {
    result.clear();
    final Matcher matcher = OUTPUT_ITEMS_GRAMMAR.matcher(text);
    while (matcher.find()) result.add(parseChange(matcher.group(1)));
  }

  private void parseOutput(String text, Collection<OutputCollection> result) {
    result.clear();
    final Matcher matcher = OUTPUT_GRAMMAR.matcher(text);

    OutputCollection lastCollection = null;
    while (matcher.find()) {
      final String match = matcher.group(1);

      if (match.charAt(0) == '{') {
        if (Character.isDigit(match.charAt(1))) { // required matches specifier
          final int spaceIndex = match.indexOf(' ');
          final int requiredMatches = Integer.parseInt(match.substring(1, spaceIndex));
          lastCollection = new OutputSet(requiredMatches);
          result.add(lastCollection);
          parseOutputItems(match.substring(spaceIndex + 1, match.length() - 1), lastCollection);
          lastCollection = null;
        } else {
          lastCollection = new OutputSet();
          result.add(lastCollection);
          parseOutputItems(match.substring(1, match.length() - 1), lastCollection);
          lastCollection = null;
        }
      } else {
        if (lastCollection == null) {
          lastCollection = new OutputList();
          result.add(lastCollection);
        }
        lastCollection.add(parseChange(match));
      }
    }
  }

  private OTransactionIndexEntry parseChange(String text) {
    OTransactionIndexChangesPerKey changes = new OTransactionIndexChangesPerKey(null);

    switch (text.charAt(0)) {
      case 'p':
        changes.add(new ORecordId(1, Integer.parseInt(text.substring(1))), OPERATION.PUT);
        return changes.getEntriesAsList().get(0);
      case 'r':
        if (text.length() == 1) {
          return null;
        } else {
          changes.add(new ORecordId(1, Integer.parseInt(text.substring(1))), OPERATION.REMOVE);
          return changes.getEntriesAsList().get(0);
        }

      case 'd':
        changes.add(null, OPERATION.REMOVE);
        return changes.getEntriesAsList().get(0);
    }

    throw new IllegalStateException("can't parse change");
  }

  private void verify(
      Iterable<OutputCollection> expected,
      Iterable<OTransactionIndexEntry> actual,
      String type,
      Iterable<OTransactionIndexEntry> input) {
    final Iterator<OTransactionIndexEntry> actualIterator = actual.iterator();

    boolean match = true;
    for (OutputCollection collection : expected)
      if (!collection.matches(actualIterator)) {
        match = false;
        break;
      }

    match &= !actualIterator.hasNext();

    if (!match)
      Assert.fail(
          "Expected '"
              + outputToString(expected)
              + "' got '"
              + sequenceToString(actual)
              + "' for "
              + type
              + " '"
              + sequenceToString(input)
              + "'.");
  }

  private String sequenceToString(Iterable<OTransactionIndexEntry> sequence) {
    final StringBuilder builder = new StringBuilder();
    for (OTransactionIndexEntry entry : sequence) builder.append(entryToString(entry)).append(' ');
    if (builder.length() > 0) builder.setLength(builder.length() - 1);
    return builder.toString();
  }

  private String outputToString(Iterable<OutputCollection> output) {
    final StringBuilder builder = new StringBuilder();
    for (OutputCollection collection : output) builder.append(collection.toString()).append(' ');
    if (builder.length() > 0) builder.setLength(builder.length() - 1);
    return builder.toString();
  }

  private interface OutputCollection extends Collection<OTransactionIndexEntry> {
    boolean matches(Iterator<OTransactionIndexEntry> actualIterator);
  }

  private static class OutputList extends ArrayList<OTransactionIndexEntry>
      implements OutputCollection {
    @Override
    public boolean matches(Iterator<OTransactionIndexEntry> actualIterator) {
      for (OTransactionIndexEntry expected : this) {
        if (!actualIterator.hasNext()) return false;
        final OTransactionIndexEntry actual = actualIterator.next();
        if (!entryEquals(expected, actual)) return false;
      }

      return true;
    }

    @Override
    public String toString() {
      final StringBuilder builder = new StringBuilder();
      for (OTransactionIndexEntry entry : this) builder.append(entryToString(entry)).append(' ');
      if (builder.length() > 0) builder.setLength(builder.length() - 1);
      return builder.toString();
    }
  }

  private static class OutputSet extends ArrayList<OTransactionIndexEntry>
      implements OutputCollection {
    private final int requiredMatches;

    public OutputSet() {
      this(-1);
    }

    public OutputSet(int requiredMatches) {
      this.requiredMatches = requiredMatches;
    }

    @Override
    public boolean matches(Iterator<OTransactionIndexEntry> actualIterator) {
      final int requiredMatches = this.requiredMatches == -1 ? this.size() : this.requiredMatches;
      final ArrayList<OTransactionIndexEntry> unmatched =
          new ArrayList<OTransactionIndexEntry>(this);
      for (int i = 0; i < requiredMatches; ++i) {
        if (!actualIterator.hasNext()) return false;
        final OTransactionIndexEntry actual = actualIterator.next();
        final int expectedIndex = unmatched.indexOf(actual);
        if (expectedIndex == -1) return false;
        final OTransactionIndexEntry expected = unmatched.get(expectedIndex);
        if (!entryEquals(expected, actual)) return false;
        unmatched.remove(expectedIndex);
      }

      return true;
    }

    @Override
    public String toString() {
      final StringBuilder builder = new StringBuilder();
      builder.append('{');
      if (requiredMatches != -1) builder.append(requiredMatches).append(' ');
      for (OTransactionIndexEntry entry : this) builder.append(entryToString(entry)).append(' ');
      if (builder.length() > 1) builder.setLength(builder.length() - 1);
      builder.append('}');
      return builder.toString();
    }
  }
}
