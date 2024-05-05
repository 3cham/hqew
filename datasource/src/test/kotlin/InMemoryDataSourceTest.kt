import io.hqew.kquery.datasource.CsvDataSource
import io.hqew.kquery.datasource.InMemoryDataSource
import java.io.File
import kotlin.test.assertEquals
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InMemoryDataSourceTest {

  @Test
  fun `test InMemoryDataSource`() {

    val csvSource =
        CsvDataSource(File("../testdata", "employee.csv").absolutePath, null, true, 1024)

    val data = csvSource.scan(listOf()).toList()

    val inMem = InMemoryDataSource(csvSource.schema(), data)

    assertEquals(inMem.data.size, 1) // contain only 1 batch since number of rows < 1024
    assertEquals(inMem.data.first().columnCount(), csvSource.schema().fields.size)

    val scannedData = inMem.scan(listOf("first_name", "last_name"))
    assertEquals(scannedData.toList().size, 1)
    assertEquals(scannedData.first().rowCount(), 4)
    assertEquals(scannedData.first().columnCount(), 2)
  }
}
