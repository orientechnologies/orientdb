import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class Foo {

  static class TheThread extends Thread {
    @Override
    public void run() {
      while (true) {

        ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/test2");
        try {
          db.open("admin", "admin");
          db.command("insert into V set name = 'foo'");
          db.command("update V set name = 'bar' where name = 'foo' limit 1");
        } catch (OException x) {
          x.printStackTrace();
        } finally {
          db.close();
        }
      }
    }
  }

  @Test
  public void test() {

    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Thread t = new TheThread();
      threads.add(t);
      t.start();
    }
    threads.forEach(x -> {
      try {
        x.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
  }
}