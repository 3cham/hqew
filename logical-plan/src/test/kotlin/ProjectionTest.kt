package io.hqew.kquery.logical

import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.datatypes.Field
import io.hqew.kquery.datatypes.Schema
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProjectionTest {

    @Test
    fun `test Projection toString`() {
        val plan = MockLogicalPlan(
            Schema(listOf(
                Field("1st", ArrowTypes.Int32Type),
                Field("2nd", ArrowTypes.StringType),
            ))
        )

        val projection = Projection(plan, listOf(Max(Column("1st")), Min(Column("2nd"))))

        assertEquals(projection.toString(), "Projection: MAX(#1st), MIN(#2nd)")
    }
}