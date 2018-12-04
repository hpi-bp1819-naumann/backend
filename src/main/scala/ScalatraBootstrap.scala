import com.amazon.deequ.backend.servlets._
import org.scalatra.LifeCycle
import javax.servlet.ServletContext
import org.slf4j.LoggerFactory
import slick.jdbc.PostgresProfile.api._
import com.mchange.v2.c3p0.ComboPooledDataSource



class ScalatraBootstrap extends LifeCycle {

  val logger = LoggerFactory.getLogger(getClass)

  val cpds = new ComboPooledDataSource

  override def init(context: ServletContext) {
    context mount (new JobManagementServlet(), "/api/jobs/*")
    val db = Database.forDataSource(cpds, None)   // create the Database object
    context mount (new SlickServlet(db), "/db/*")   // create and mount the Scalatra application
  }


  private def closeDbConnection() {
    logger.info("Closing postgres connection")
    cpds.close
  }

  override def destroy(context: ServletContext) {
    super.destroy(context)
    closeDbConnection
  }
}
