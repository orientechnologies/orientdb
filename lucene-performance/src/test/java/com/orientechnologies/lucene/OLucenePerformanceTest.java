package com.orientechnologies.lucene;

import org.apache.lucene.index.IndexReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;

/**
 * Created by frank on 12/11/2016.
 */
@RunWith(Parameterized.class)
public class OLucenePerformanceTest {



  private final long docsPerIndex;
  @Rule
  public TestName testName = new TestName();

  public OLucenePerformanceTest(long docsPerIndex) {
    this.docsPerIndex = docsPerIndex;
  }

  @Parameters
  public static Collection<Object> docsPerIndex() {
    return Arrays.asList(Long.MAX_VALUE, 1000L, 5000L, 10000L);

  }

  @Before
  public void setUp() throws Exception {

    //    OFileUtils.deleteRecursively(Paths.get("./target/lucene/" + testName.getMethodName()).toFile());

  }

  @Test
  public void testPerformance() throws Exception {

    System.out.println("___________________________");
    System.out.println("docs per idex :: " + docsPerIndex);
    String baseIndexPath = "./target/lucene/" + testName.getMethodName();
    OLuceneIndexWritables writables = new OLuceneIndexWritables(docsPerIndex, baseIndexPath);

    OLucenePerformanceJsonIndexingPipeline pipeline = new OLucenePerformanceJsonIndexingPipeline(writables);

    pipeline.process("/Users/frank/Downloads/reviews_Electronics_5.json");

    OLuceneIndexSearcher searcher = new OLuceneIndexSearcher(baseIndexPath);

    IndexReader reader = searcher.reader();

    performQuery(searcher, 10);

  }

  private void performQuery(OLuceneIndexSearcher searcher, int repeat) {
    if (repeat > 0) {                           // repeat & time as benchmark
      for (int i = 0; i < repeat; i++) {
        LocalTime start = LocalTime.now();
        IndexReader reader = searcher.reader();
        searcher.search("+good +fast -well -today", 100);
        LocalTime end = LocalTime.now();
        System.out.println("round:: " + i + " Time: " + Duration.between(start, end).toMillis() + "ms");
      }
    }
  }

}
