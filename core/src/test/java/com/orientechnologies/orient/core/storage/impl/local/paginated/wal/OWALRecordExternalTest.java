package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Created by Enrico Risa on 10/08/2017.
 */
public class OWALRecordExternalTest {

  private ODatabaseInternal db;
  OrientDB orientDB;

  @Before
  public void beforeClass() {

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, TestRecord.class);
    final String dbName = getDBName();
    openAndCreateOrientDB(dbName);
    db = initDB(dbName);
  }

  protected String getDBName() {
    return OWALRecordExternalTest.class.getSimpleName();
  }

  protected String getNewDBName() {
    return OWALRecordExternalTest.class.getSimpleName() + "New";
  }

  protected ODatabaseInternal initDB(String dbName) {
    return (ODatabaseInternal) orientDB.open(dbName, "admin", "admin");
  }

  protected void openAndCreateOrientDB(String dbName) {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());

    orientDB.createIfNotExists(dbName, ODatabaseType.PLOCAL);
  }

  @After
  public void afterClass() {

    if(!db.isClosed()) {
      db.close();
    }
    orientDB.drop(OWALRecordExternalTest.class.getSimpleName());
    orientDB.close();

  }

  @Test
  public void testExternalWALRecord() throws IOException {

    logRecord();

    db.close();

    orientDB.close();

    beforeClass();

    OWriteAheadLog writeAheadLog = ((OAbstractPaginatedStorage) db.getStorage()).getWALInstance();

    OLogSequenceNumber lsn = writeAheadLog.begin();
    int recordsCount = 0;
    while (lsn != null) {
      OWALRecord read = writeAheadLog.read(lsn);

      if (read instanceof TestRecord) {
        TestRecord testRecord = (TestRecord) read;
        Assert.assertEquals(10, (long) testRecord.value);
        recordsCount++;
      }
      lsn = writeAheadLog.next(lsn);

    }
    Assert.assertEquals(1, recordsCount);

  }

  private void logRecord() throws IOException {
    OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) db.getStorage();
    OAtomicOperationsManager manager = storage.getAtomicOperationsManager();
    OAtomicOperation atomic = manager.startAtomicOperation(new FakeComponent(storage), true);
    atomic.addExternalLog(new TestRecord(atomic.getOperationUnitId(), 10));
    manager.endAtomicOperation(false, null);

    storage.getWALInstance().flush();

  }

  @Test
  public void testRestore() throws IOException {

    logRecord();




    final String buildDirectory = System.getProperty("buildDirectory", ".");
    File buildDir = new File(buildDirectory);
    final String dbPath = buildDir.getAbsolutePath() + File.separator + getDBName();
    final String newDBPath = buildDir.getAbsolutePath() + File.separator + getNewDBName();

    final Path sourcePath = Paths.get(dbPath);
    final Path targetPath = Paths.get(newDBPath);

    Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {

      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        Files.createDirectories(targetPath.resolve(sourcePath.relativize(dir)));
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

        Path resolved = targetPath.resolve(sourcePath.relativize(file));
        String fileName = file.getFileName().toString();
        if (fileName.endsWith(".wal")) {
          String newDB = fileName.replaceAll(getDBName(), getNewDBName());
          resolved = resolved.getParent().resolve(newDB);
        }
        if (!fileName.equalsIgnoreCase("dirty.fl")) {
          Files.copy(file, resolved);
        }
        return FileVisitResult.CONTINUE;
      }
    });

    db.close();

    orientDB.close();

    openAndCreateOrientDB(OWALRecordExternalTest.class.getSimpleName());

    ODatabaseInternal open = initDB(getNewDBName());

    open.close();

    orientDB.drop(getNewDBName());

    Assert.assertEquals(true, TestRecord.restored);

  }

  public static final class FakeComponent extends ODurableComponent {

    public FakeComponent(OAbstractPaginatedStorage storage) {
      this(storage, "", "", "");
    }

    public FakeComponent(OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
      super(storage, name, extension, lockName);
    }
  }

  public static final class TestRecord extends OOperationUnitBodyRecordExternal {

    static boolean restored = false;
    Integer value;

    public TestRecord() {
    }

    public TestRecord(OOperationUnitId operationUnitId, Integer value) {
      super(operationUnitId);
      this.value = value;

    }

    @Override
    public int toStream(byte[] content, int offset) {
      offset = super.toStream(content, offset);
      OIntegerSerializer.INSTANCE.serializeNative(value, content, offset);
      offset += OIntegerSerializer.INT_SIZE;
      return offset;
    }

    @Override
    public int fromStream(byte[] content, int offset) {
      offset = super.fromStream(content, offset);
      value = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;
      return offset;
    }

    @Override
    public boolean isUpdateMasterRecord() {
      return false;
    }

    @Override
    public int serializedSize() {
      int size = super.serializedSize();
      size += OIntegerSerializer.INT_SIZE;
      return size;
    }

    @Override
    public void restore(OAbstractPaginatedStorage storage) {
      restored = true;
    }
  }
}
