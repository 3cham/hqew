package io.hqew.kquery.planner

import io.hqew.kquery.logical.Aggregate
import io.hqew.kquery.logical.Column
import io.hqew.kquery.logical.Max
import io.hqew.kquery.logical.Scan
import org.junit.jupiter.api.Assertions.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryPlannerTest {
    @Test
    fun `create physical plan for scan`() {
        // given
        val testee = QueryPlanner()
        val plan = Scan(
            "employee",
            readEmployeeTestData(),
            listOf("id"),
        )
        // when
        val result = testee.createPhysicalPlan(plan).execute().first()

        // then
        assertEquals(4, result.rowCount())
        for (i in 0..<4) {
            assertEquals((i + 1).toLong(), result.field(0).getValue(i))
        }
    }

    @Test
    fun `create physical plan for aggregate query`() {
        // given
        val testee = QueryPlanner()
        val input = Scan(
            "employee",
            readEmployeeTestData(),
            listOf("first_name", "last_name", "state", "salary"),
        )

        val maxSalaryGroupByState = Aggregate(input,
            groupExpr = listOf(Column("state")),
            aggregateExpr = listOf(Max(Column("salary"))),
        )

        // when
        val result = testee.createPhysicalPlan(maxSalaryGroupByState).execute().first()

        // then
        assertEquals(3, result.rowCount()) // Expecting three states in the first batch
        val states = result.field(0)
        val maxSalaries = result.field(1)

        for (i in 0..<3) {
            if ((states.getValue(i) as? String) == "CO") {
                assertEquals(11500.0, maxSalaries.getValue(i) as? Double)
            }
        }
    }
}