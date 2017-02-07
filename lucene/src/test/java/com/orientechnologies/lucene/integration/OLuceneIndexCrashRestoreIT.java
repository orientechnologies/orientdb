package com.orientechnologies.lucene.integration;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.junit.*;

import java.io.File;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

public class OLuceneIndexCrashRestoreIT {

  private AtomicLong          idGen;
  private ODatabaseDocumentTx testDocumentTx;

  private ExecutorService executorService;
  private Process         serverProcess;
  private List<String>    names;

  @Before
  public void beforeMethod() throws Exception {
    executorService = Executors.newCachedThreadPool();
    idGen = new AtomicLong();
    spawnServer();

    testDocumentTx = new ODatabaseDocumentTx("remote:localhost:3900/testLuceneCrash");
    testDocumentTx.open("admin", "admin");

    //names to be used for person to be indexd
    names = Arrays.asList("John", "Robert", "Jane", "andrew", "Scott", "luke", "Enriquez", "Luis", "Gabriel", "Sara");

  }

  public void spawnServer() throws Exception {
    OLogManager.instance().installCustomFormatter();
    OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(1000000);
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(3);
    OGlobalConfiguration.FILE_LOCK.setValue(false);

    final String buildDirectory = "./target/testLuceneCrash";

    final File buildDir = new File(buildDirectory);
    if (buildDir.exists()) {
      OFileUtils.deleteRecursively(buildDir);
    }

    buildDir.mkdirs();

    final File mutexFile = new File(buildDir, "mutex.ct");
    final RandomAccessFile mutex = new RandomAccessFile(mutexFile, "rw");
    mutex.seek(0);
    mutex.write(0);

    String javaExec = System.getProperty("java.home") + "/bin/java";
    javaExec = new File(javaExec).getCanonicalPath();

    ProcessBuilder processBuilder = new ProcessBuilder(javaExec,
        "-Xmx2048m",
        "-XX:MaxDirectMemorySize=512g",
        "-classpath",
        System.getProperty("java.class.path"),
        "-DmutexFile=" + mutexFile.getAbsolutePath(),
        "-DORIENTDB_HOME=" + buildDirectory,
        RemoteDBRunner.class.getName());

    processBuilder.inheritIO();

    serverProcess = processBuilder.start();

    System.out.println(": Wait for server start");
    boolean started = false;
    do {
      TimeUnit.SECONDS.sleep(5);
      mutex.seek(0);
      started = mutex.read() == 1;
    } while (!started);

    mutex.close();
    mutexFile.delete();
    System.out.println(": Server was started");
  }

  @After
  public void tearDown() {
    File buildDir = new File("./target/databases");
    OFileUtils.deleteRecursively(buildDir);
    Assert.assertFalse(buildDir.exists());
  }

  @Test
  @Ignore
  public void testEntriesAddition() throws Exception {
    createSchema(testDocumentTx);

    System.out.println("Start data propagation");

    List<Future> futures = new ArrayList<Future>();
    for (int i = 0; i < 4; i++) {
      futures.add(executorService.submit(new DataPropagationTask(testDocumentTx)));
    }

    System.out.println("Wait for 5 minutes");
    TimeUnit.SECONDS.sleep(5);

    //test query
    // verify that the keyword analyzer is doing is job
    testDocumentTx.activateOnCurrentThread();

    //wildcard will not work
    List<ODocument> res = testDocumentTx.query(new OSQLSynchQuery<ODocument>("select from Person where name lucene 'Rob*' "));

    assertThat(res).hasSize(0);

    //plain name fetch docs
    testDocumentTx.activateOnCurrentThread();
    res = testDocumentTx.query(new OSQLSynchQuery<ODocument>("select from Person where name lucene 'Robert' LIMIT 20"));

    assertThat(res).hasSize(20);

    //crash the server
    // this works only on java8
    serverProcess.destroyForcibly();

    serverProcess.waitFor();

    System.out.println("Process was CRASHED");

    //stop data pumpers
    for (Future future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        future.cancel(true);
      }
    }

    System.out.println("All loaders done");

    //now we start embedded
    System.out.println("START AGAIN");

    //start embedded
    OServer server = OServerMain.create();
    InputStream conf = RemoteDBRunner.class.getResourceAsStream("index-crash-config.xml");

    server.startup(conf);
    server.activate();

    while (!server.isActive()) {
      System.out.println("server active = " + server.isActive());
      TimeUnit.SECONDS.sleep(1);
    }

    //test query
    testDocumentTx.activateOnCurrentThread();

    testDocumentTx.getMetadata().reload();

    OIndex<?> index = testDocumentTx.getMetadata().getIndexManager().getIndex("Person.name");
    assertThat(index).isNotNull();

    //sometimes the metadata is null!!!!!
    assertThat((Iterable<? extends Map.Entry<String, Object>>) index.getMetadata()).isNotNull();

    assertThat(index.getMetadata().<String>field("default")).isNotNull();
    assertThat(index.getMetadata().<String>field("default"))
        .isEqualTo("org.apache.lucene.analysis.core.KeywordAnalyzer");
    assertThat(index.getMetadata().<String>field("unknownKey"))
        .isEqualTo("unknownValue");

    //sometimes it is not null, and all works fine
    res = testDocumentTx.query(new OSQLSynchQuery<ODocument>("select from Person where name lucene 'Rob*' "));

    assertThat(res).hasSize(0);

    testDocumentTx.activateOnCurrentThread();
    res = testDocumentTx.query(new OSQLSynchQuery<ODocument>("select from Person where name lucene 'Robert' LIMIT 20"));

    assertThat(res).hasSize(20);

    //shutdown embedded
    server.shutdown();

  }

  private void createSchema(ODatabaseDocumentTx db) {
    db.activateOnCurrentThread();

    System.out.println("create index for db:: " + db.getURL());

    db.command(new OCommandSQL("Create class Person")).execute();
    db.command(new OCommandSQL("Create property Person.name STRING")).execute();
    db.command(new OCommandSQL(
        "Create index Person.name on Person(name) fulltext engine lucene metadata {'default':'org.apache.lucene.analysis.core.KeywordAnalyzer', 'unknownKey':'unknownValue'}"))
        .execute();
    db.getMetadata().getIndexManager().reload();

    System.out.println(db.getMetadata().getIndexManager().getIndex("Person.name").getConfiguration().toJSON());
  }

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {
      System.out.println("prepare server");
      OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(3);
      OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(100000000);

      System.out.println("create server instance");
      OServer server = OServerMain.create();
      InputStream conf = RemoteDBRunner.class.getResourceAsStream("index-crash-config.xml");

      server.startup(conf);
      server.activate();

      final String mutexFile = System.getProperty("mutexFile");
      System.out.println("mutexFile = " + mutexFile);

      final RandomAccessFile mutex = new RandomAccessFile(mutexFile, "rw");
      mutex.seek(0);
      mutex.write(1);
      mutex.close();
    }
  }

  public class DataPropagationTask implements Callable<Void> {
    private ODatabaseDocumentTx testDB;

    public DataPropagationTask(ODatabaseDocumentTx testDocumentTx) {
      this.testDB = new ODatabaseDocumentTx(testDocumentTx.getURL());

    }

    @Override
    public Void call() throws Exception {
      testDB.open("admin", "admin");

      try {
        while (true) {
          long id = idGen.getAndIncrement();
          long ts = System.currentTimeMillis();

          if (id % 1000 == 0) {
            System.out.println(Thread.currentThread().getName() + " inserted:: " + id);
            testDB.commit();
          }
          int nameIdx = (int) (id % names.size());

          ODatabaseRecordThreadLocal.INSTANCE.set(testDB);
          for (int i = 0; i < 10; i++) {
            String insert = "insert into person (name) values ('" + names.get(nameIdx) + "')";
            testDB.command(new OCommandSQL(insert)).execute();
          }

        }
      } catch (Exception e) {
        throw e;
      } finally {

        try {
          testDB.activateOnCurrentThread();
          testDB.close();
        } catch (Exception e) {
        }
      }

    }
  }
}
