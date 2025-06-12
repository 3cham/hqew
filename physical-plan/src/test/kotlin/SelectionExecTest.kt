import io.hqew.kquery.datasource.CsvDataSource
import io.hqew.kquery.datasource.InMemoryDataSource
import io.hqew.kquery.datatypes.Schema
import io.hqew.kquery.physical.ScanExec
import io.hqew.kquery.physical.SelectionExec
import io.hqew.kquery.physical.expressions.BooleanExpression
import io.hqew.kquery.physical.expressions.ColumnExpression
import io.hqew.kquery.physical.expressions.EqExpression
import io.hqew.kquery.physical.expressions.GtEqExpression
import io.hqew.kquery.physical.expressions.LiteralDoubleExpression
import java.io.File
import org.junit.Test
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.to

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SelectionExecTest {

    @Test
    fun `test selection execution`() {
        // Here you would set up your test environment, create a mock input plan,
        // and an expression to evaluate. Then you would execute the SelectionExec
        // and verify the results against expected output.

        val csvSource =
            CsvDataSource(
                File("../testdata", "employee.csv").absolutePath,
                schema= Schema(
                    listOf(
                        io.hqew.kquery.datatypes.Field("id", io.hqew.kquery.datatypes.ArrowTypes.Int64Type),
                        io.hqew.kquery.datatypes.Field("first_name", io.hqew.kquery.datatypes.ArrowTypes.StringType),
                        io.hqew.kquery.datatypes.Field("last_name", io.hqew.kquery.datatypes.ArrowTypes.StringType),
                        io.hqew.kquery.datatypes.Field("state", io.hqew.kquery.datatypes.ArrowTypes.StringType),
                        io.hqew.kquery.datatypes.Field("job_title", io.hqew.kquery.datatypes.ArrowTypes.StringType),
                        io.hqew.kquery.datatypes.Field("salary", io.hqew.kquery.datatypes.ArrowTypes.DoubleType)
                    )
                ), true, 1024)

        val input = ScanExec(
            ds = csvSource,
            projection = listOf("first_name", "last_name", "salary")
        )

        val selectionExec = SelectionExec(input, expr = GtEqExpression(
            l = ColumnExpression(2),
            r = LiteralDoubleExpression(11000.0)
        )
        )
        val result = selectionExec.execute()

        assertEquals(result.first().rowCount(), 3) // Expecting three rows to match the condition
    }

}