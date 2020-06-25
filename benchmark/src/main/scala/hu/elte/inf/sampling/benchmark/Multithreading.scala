package hu.elte.inf.sampling.benchmark

import java.util.concurrent.{ExecutorService, Executors}

import hu.elte.inf.sampling.core.Logger

import scala.concurrent.ExecutionContext

trait Multithreading extends Logger {
  val service = Executors.newFixedThreadPool(10)
  implicit val ec: ExecutionContext = new ExecutionContext {
    val threadPool: ExecutorService = service

    override def reportFailure(cause: Throwable): Unit = {
      logError(cause.toString)
    }

    override def execute(runnable: Runnable): Unit = threadPool.submit(runnable)
  }
}
