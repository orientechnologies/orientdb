import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.junit.Ignore;
import org.junit.Test;

public class BlockingTest {

  @Test
  @Ignore
  public void test() {
    OrientDB db =
        new OrientDB(
            "remote:localhost:2424;localhost:2425;localhost:2426", OrientDBConfig.defaultConfig());

    for (int i = 0; i < 20; i++) {
      ODatabaseSession session = db.open("Reactome", "admin", "admin");
      session.newVertex().save();
      session.close();
    }
    db.close();
  }
}
