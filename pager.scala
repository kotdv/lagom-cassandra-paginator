import com.datastax.driver.core.{PagingState, Row, SimpleStatement, Statement}
import com.lightbend.lagom.scaladsl.persistence.cassandra.CassandraSession

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

package object pager {
  final case class Page[A](next: Option[String], results: Seq[A])

  def pager[A](query: String, next: Option[String], size: Option[Int],
               mapper: Row => A)(implicit session: CassandraSession, ec: ExecutionContext): Future[Page[A]] = {

    val statement = new SimpleStatement(query)
    pager(statement, next, size, mapper)
  }

  def pager[A](statement: Statement, next: Option[String], size: Option[Int],
               mapper: Row => A)(implicit session: CassandraSession, ec: ExecutionContext): Future[Page[A]] = {

    statement.setFetchSize(size.getOrElse(20))

    if (next.isDefined)
      statement.setPagingState(PagingState.fromString(next.get))

    import akka.persistence.cassandra.ListenableFutureConverter
    import scala.collection.JavaConverters._

    session.underlying() flatMap { s =>
      s.executeAsync(statement).asScala map { rs =>
        val remaining = rs.getAvailableWithoutFetching

        val rows = rs.asScala.toSeq
        val mappedRows = for (i <- 0 until remaining) yield mapper(rows(i))

        val n = Try(rs.getExecutionInfo.getPagingState.toString).toOption

        Page[A](n, mappedRows)
      }
    }
  }
}
