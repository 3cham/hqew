package io.hqew.kquery.physical

import io.hqew.kquery.datatypes.Schema
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import io.hqew.kquery.physical.expressions.MaxExpression
import io.hqew.kquery.physical.expressions.ColumnExpression
import io.hqew.kquery.datatypes.Field
import io.hqew.kquery.datatypes.ArrowTypes


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HashAggregationExecTest {

    @Test
    fun `test hash aggregation execution`() {
        val input = ScanExec(
            ds = readEmployeeTestData(),
            projection = listOf("first_name", "last_name", "state", "salary")
        )

        // max salary group by state
        val hashAggExec = HashAggregateExec(
            input,
            groupExpr = listOf(ColumnExpression(2)),
            aggregateExpr = listOf(MaxExpression(ColumnExpression(3))),
            schema = Schema(
                fields = listOf(Field("state", ArrowTypes.StringType), Field("max_salary", ArrowTypes.DoubleType)),
            )
        )
        val result = hashAggExec.execute()

        val rowCount = result.first().rowCount()

        assertEquals(3, rowCount) // Expecting three states in the first batch
        val states = result.first().field(0)
        val max_salaries = result.first().field(1)

        for (i in 0..rowCount) {
            if ((states.getValue(i) as? String) == "CO") {
                assertEquals(11500.0, max_salaries.getValue(i) as? Double)
            }
        }
    }
}
