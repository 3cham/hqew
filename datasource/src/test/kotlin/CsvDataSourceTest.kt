import io.hqew.kquery.datasource.CsvDataSource
import java.io.File
import kotlin.test.assertEquals
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CsvDataSourceTest {

  val dir = "../testdata"
  @Test
  fun `read csv with no projection`() {
    val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1024)

    val headers = listOf("id", "first_name", "last_name", "state", "job_title", "salary")

    val results = csv.scan(listOf())

    results.forEach {
      val field = it.fields[0]

      assert(field.size() == 4) // 4 rows
      assert(it.fields.size == headers.size) // 6 columns

      assert(it.schema.fields.size == headers.size)
      assert(it.schema.fields.map { f -> f.name } == headers)
    }
  }

  @Test
  fun `read csv with projection`() {
    val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1024)

    val results = csv.scan(listOf("id", "salary"))

    results.forEach {
      val field = it.fields[0]

      assert(field.size() == 4) // 4 rows
      assert(it.fields.size == 2) // 2 columns

      assert(it.schema.fields.size == 2)
      assert(it.schema.fields.map { f -> f.name } == listOf("id", "salary"))
    }
  }

  @Test
  fun `read csv with projection and small batch`() {
    val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1)

    val results = csv.scan(listOf("id", "salary"))

    results.forEach {
      val field = it.fields[0]

      assert(field.size() == 1) // 1 row per batch
      assert(it.fields.size == 2) // 2 columns

      assert(it.schema.fields.size == 2)
      assert(it.schema.fields.map { f -> f.name } == listOf("id", "salary"))
    }
  }

  @Test
  fun `read csv without headers`() {
    val csv = CsvDataSource(File(dir, "employee_no_header.csv").absolutePath, null, false, 1024)

    val headers = listOf("field_1", "field_2", "field_3", "field_4", "field_5", "field_6")
    val results = csv.scan(listOf())

    results.forEach {
      val field = it.fields[0]

      assertEquals(field.size(), 4)
      assert(it.fields.size == headers.size)

      assert(it.schema.fields.size == headers.size)
      assertEquals(it.schema.fields.map { f -> f.name }, headers)
    }
  }

  @Test
  fun `read tsv without headers`() {
    val csv = CsvDataSource(File(dir, "employee_no_header.tsv").absolutePath, null, false, 1024)

    val headers = listOf("field_1", "field_2", "field_3", "field_4", "field_5", "field_6")
    val results = csv.scan(listOf())

    results.forEach {
      val field = it.fields[0]

      assertEquals(field.size(), 4)
      assert(it.fields.size == headers.size)

      assert(it.schema.fields.size == headers.size)
      assertEquals(it.schema.fields.map { f -> f.name }, headers)
    }
  }
}
