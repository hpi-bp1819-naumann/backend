import com.amazon.scalatra4deequ.Scalatra4DeequServlet
import org.scalatra.LifeCycle
import javax.servlet.ServletContext

class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext) {
    //val todos = collection.mutable.Map[Integer, Todo]()
    context mount (new Scalatra4DeequServlet(), "/*")
    }
}
