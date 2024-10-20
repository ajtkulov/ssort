package ssort

import java.util.concurrent._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.language.implicitConversions

object ExecutionPool {
  val defaultThreadPoolSize: Int = 4

  val threadFactory = new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r)
      thread.setDaemon(true)
      thread
    }
  }

  def createSingletonExecutionContext: ExecutionContextExecutorService = createExecutionContext(1)

  def createExecutionContext(threadPoolSize: Int = defaultThreadPoolSize): ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(threadPoolSize, threadFactory))
}
