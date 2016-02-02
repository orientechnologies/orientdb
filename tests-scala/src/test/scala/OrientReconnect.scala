import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.query.OQuery
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.orientechnologies.orient.server.OServerMain
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory
import org.scalatest._
import org.scalatest.BeforeAndAfterAll
import com.orientechnologies.orient.core.exception._
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph
import org.apache.commons.configuration._
import com.orientechnologies.orient.core.db.document._

import gremlin.scala._
import org.apache.tinkerpop.gremlin.orientdb._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.concurrent.Await

class OrientReconnect extends WordSpec with ShouldMatchers with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    stopServer
  }

  def startServer:Unit = {
    OServerMain.main(Array.empty[String])
    createDb
  }
  def stopServer:Unit = OServerMain.server().shutdown()


  val url = "remote:localhost"
  val user = "root"
  val pass = "somepass"
  val dbName = "db-test-failing"
  val dbType = "graph"
  val dbStorage = "memory"

  def createDb: Unit = {
    new OServerAdmin(url)
      .connect(user, pass)
      .createDatabase(dbName, dbType, dbStorage).close()
  }

  def getGraph: gremlin.scala.ScalaGraph[OrientGraph] = {

    new OrientGraphFactory(url + "/" + dbName)
      .setupPool(5)
      .getNoTx.asScala
  }

  "OrientGraph" should {
    "reconnect to db once the connection is working again" in {
      startServer
      val graph = getGraph

      graph.addVertex("label")
      graph.V.count.head

      stopServer

      intercept[Exception] {
        graph.V.count.head
      }

      startServer

      graph.addVertex("label")
      graph.V.count.head
    }
  }
}


