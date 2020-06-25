package hu.elte.inf.sampling.core

import scala.io.Source

trait Logger {
  def logDebug(s: => String): Unit = {
    if (Logger.logLevel >= 3) {
      println(s"${Console.WHITE}[${Utils.dateTime} - DEBUG]: $s${Console.RESET}")
    }
  }

  def logInfo(s: => String): Unit = {
    if (Logger.logLevel >= 2) {
      println(s"${Console.RESET}[${Utils.dateTime} - INFO]: $s${Console.RESET}")
    }
  }

  def logWarning(s: => String): Unit = {
    if (Logger.logLevel >= 1) {
      println(s"${Console.YELLOW}[${Utils.dateTime} - WARN]: $s${Console.RESET}")
    }
  }

  def logError(s: => String): Unit = {
    println(s"${Console.RED}[${Utils.dateTime} - ERROR]: $s${Console.RESET}")
  }
}

object Logger {
  val logLevel: Int = Source.fromResource("log.conf").mkString.toLowerCase match {
    case "debug" => 3
    case "info" => 2
    case "warning" => 1
    case "error" => 0
    case _ => -1
  }
}