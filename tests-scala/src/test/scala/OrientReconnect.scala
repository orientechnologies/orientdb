import com.orientechnologies.common.exception.OException
import com.orientechnologies.common.io.OIOException
import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.server.OServerMain
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory
import org.scalatest._
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph
import gremlin.scala._

class OrientReconnect extends WordSpec with ShouldMatchers {


  def startServer: Unit =
    OServerMain.create(false).startup().activate()

  def stopServer: Unit =
    if (OServerMain.server() != null)
      OServerMain.server().shutdown()

  val url = "remote:localhost"
  val user = "root"
  val pass = "somepass"
  val dbName = "db-test-failing"
  val dbType = "graph"
  val dbStorage = "memory"

  def createDb: Unit =
    new OServerAdmin(url)
      .connect(user, pass)
      .createDatabase(dbName, dbType, dbStorage).close()

  def getGraph: gremlin.scala.ScalaGraph =
    new OrientGraphFactory(url + "/" + dbName)
      .setupPool(5)
      .getNoTx.asScala

  "OrientGraph" should {
    "reconnect to db once the connection is working again" in {
      startServer
      createDb
      val graph = getGraph

      graph.addVertex("label")
      graph.V.count.head

      stopServer

      intercept[OException] {
        graph.V.count.head
      }

      startServer

      graph.addVertex("label")
      graph.V.count.head
    }
  }
}

