
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor

object main extends App {
    val context = ComponentContext()
    ThreadExecutor.waitForShutdown()
}
