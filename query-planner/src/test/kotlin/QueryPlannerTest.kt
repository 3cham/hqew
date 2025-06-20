package io.hqew.kquery.planner

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
        val result = testee.createPhysicalPlan(plan).execute()

        // then
        assertEquals(4, result.first().rowCount())
        for (i in 0..<4) {
            assertEquals(i + 1, result.first().field(0).getValue(i))
        }
    }

    @Test
    fun createPhysicalExpr() {
    }

}