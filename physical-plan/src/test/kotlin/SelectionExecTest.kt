package io.hqew.kquery.physical

import io.hqew.kquery.datasource.CsvDataSource
import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.datatypes.Field
import io.hqew.kquery.datatypes.Schema
import io.hqew.kquery.physical.expressions.ColumnExpression
import io.hqew.kquery.physical.expressions.GtEqExpression
import io.hqew.kquery.physical.expressions.LiteralDoubleExpression
import java.io.File
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SelectionExecTest {

    @Test
    fun `test selection execution`() {
        // Here you would set up your test environment, create a mock input plan,
        // and an expression to evaluate. Then you would execute the SelectionExec
        // and verify the results against expected output.

        val input = ScanExec(
            ds = readEmployeeTestData(),
            projection = listOf("first_name", "last_name", "salary")
        )

        val selectionExec = SelectionExec(
            input,
            expr = GtEqExpression(
                l = ColumnExpression(2),
                r = LiteralDoubleExpression(11000.0),
            )
        ) // Filtering employees with salary >= 11000.0
        val result = selectionExec.execute()

        assertEquals(3, result.first().rowCount()) // Expecting three rows to match the condition
    }
}

fun readEmployeeTestData(): CsvDataSource = CsvDataSource(
    File("../testdata", "employee.csv").absolutePath,
    schema = Schema(
        listOf(
            Field("id", ArrowTypes.Int64Type),
            Field("first_name", ArrowTypes.StringType),
            Field("last_name", ArrowTypes.StringType),
            Field("state", ArrowTypes.StringType),
            Field("job_title", ArrowTypes.StringType),
            Field("salary", ArrowTypes.DoubleType)
        )
    ), true, 1024
)