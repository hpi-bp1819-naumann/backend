import com.amazon.deequ.backend.servlets._
import org.scalatra.LifeCycle
import javax.servlet.ServletContext


class ScalatraBootstrap extends LifeCycle {

  override def init(context: ServletContext) {
    context mount (new JobManagementServlet(), "/api/jobs/*")
    context mount (new DBAccessServlet(), "/db/*")
  }
}
