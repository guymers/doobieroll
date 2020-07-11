package doobierolltest.sql

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext

import cats.effect.Blocker
import cats.effect.ContextShift
import cats.effect.IO
import cats.kernel.Eq
import doobie.hikari.HikariTransactor
import doobie.syntax.connectionio._
import doobie.syntax.stream._
import doobie.util.Read
import doobie.util.query.Query0
import doobierolltest.Naive
import doobierolltest.TestDataInstances.Infallible
import doobierolltest.model._
import fs2.Stream
import io.circe.Decoder
import org.openjdk.jmh.annotations._
import shapeless.::
import shapeless.HNil
import skunk.Session

// docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=password -e POSTGRES_DB=doobieroll -v "$(pwd)/modules/bench/src/main/resources/sql/tables.sql:/docker-entrypoint-initdb.d/01-tables.sql" -v "$(pwd)/modules/bench/src/main/resources/sql/data.sql:/docker-entrypoint-initdb.d/02-data.sql" postgres:12

object Comparison {
  import doobie.postgres.implicits.UuidType

  private implicit val eqDbCompany: Eq[DbCompany] = Eq.fromUniversalEquals
  private implicit val eqDbDepartment: Eq[DbDepartment] = Eq.fromUniversalEquals

  private implicit val decoderEmployee: Decoder[Employee] = io.circe.generic.semiauto.deriveDecoder
  private implicit val decoderDepartment: Decoder[Department] = io.circe.generic.semiauto.deriveDecoder
  private implicit val decoderCompany: Decoder[Company] = io.circe.generic.semiauto.deriveDecoder

  private val sql = """
    SELECT
      company_id,
      company.name,

      department_id,
      company_id,
      department.name,

      employee_id,
      department_id,
      employee.name
    FROM company
    JOIN department USING (company_id)
    JOIN employee USING (department_id)
  """
  private val query = Query0[DbCompany :: DbDepartment :: DbEmployee :: HNil](sql)
  private val queryOrdered = Query0[DbCompany :: DbDepartment :: DbEmployee :: HNil](
    sql concat " ORDER BY company_id, department_id, employee_id"
  )
  private val querySkunk = {
    import skunk.codec.all._
    import skunk.syntax.stringcontext._

    val decoderDbCompany = (uuid ~ text).gimap[DbCompany]
    val decoderDbDepartment = (uuid ~ uuid ~ text).gimap[DbDepartment]
    val decoderDbEmployee = (uuid ~ uuid ~ text).gimap[DbEmployee]
    val decoder = (decoderDbCompany ~ decoderDbDepartment ~ decoderDbEmployee).map { case ((c, d), e) =>
      c :: d :: e :: HNil
    }

    val fragment: skunk.Fragment[skunk.Void] = sql"""
      SELECT
        company_id,
        company.name,

        department_id,
        company_id,
        department.name,

        employee_id,
        department_id,
        employee.name
      FROM company
      JOIN department USING (company_id)
      JOIN employee USING (department_id)
    """
    fragment.query(decoder)
  }

  private val sqlJSON = """
    SELECT
      jsonb_build_object(
        'id', company_id,
        'name', company.name,
        'departments', (
          SELECT json_agg(jsonb_build_object(
            'id', department_id,
            'name', department.name,
            'employees', (
              SELECT json_agg(jsonb_build_object(
                'id', employee_id,
                'name', employee.name
              ))
              FROM employee
              WHERE employee.department_id = department.department_id
            )
          ))
          FROM department
          WHERE department.company_id = company.company_id
        )
      )
    FROM company
  """
  private val queryJSON = {
    import doobie.postgres.circe.jsonb.implicits._
    val read = Read.fromGet(pgDecoderGetT[Company])
    Query0(sqlJSON)(read)
  }
}

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class Comparison {
  import Comparison._

  private val (transactor, _) = {
    val ec = ExecutionContext.global
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    HikariTransactor.newHikariTransactor[IO](
      driverClassName = "org.postgresql.Driver",
      url = "jdbc:postgresql://localhost:5432/doobieroll",
      user = "postgres",
      pass = "password",
      ec,
      Blocker.liftExecutionContext(ec)
    )
  }.allocated.unsafeRunSync()

  private val (session, _) = {
    import natchez.Trace.Implicits.noop
    val ec = ExecutionContext.global
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    Session.pooled[IO](
      host = "localhost",
      port = 5432,
      user = "postgres",
      database = "doobieroll",
      password = Some("password"),
      max = 5,
    )
  }.allocated.unsafeRunSync()

  @Benchmark
  def naive: Iterable[Company] = {
    query.to[Vector].transact(transactor).map { results =>
      Naive.assembleUngrouped(results)
    }.unsafeRunSync()
  }

  @Benchmark
  def roll: Vector[Company] = {
    query.to[Vector].transact(transactor).map { results =>
      Infallible.companyAssembler.assemble(results)
    }.unsafeRunSync()
  }

  @Benchmark
  def fs2: Vector[Company] = {
    val stream = queryOrdered.stream.transact(transactor)
      .groupAdjacentBy(_.head)
      .map { case (company, chunk) =>
        val departments = Stream.chunk(chunk).groupAdjacentBy(_.tail.head).map { case (department, chunk) =>
          val employees = chunk.map(t => Employee.fromDb(t.tail.tail.head))
          Department.fromDb(department, employees.toVector)
        }.compile.toVector
        Company.fromDb(company, departments)
      }
    stream.compile.toVector.unsafeRunSync()
  }

  @Benchmark
  def json: Vector[Company] = {
    queryJSON.to[Vector].transact(transactor).unsafeRunSync()
  }

  @Benchmark
  def skunkNaive: Iterable[Company] = {
    session.use { s =>
      s.execute(querySkunk)
    }.map { results =>
      Naive.assembleUngrouped(results.toVector)
    }.unsafeRunSync()
  }

}
