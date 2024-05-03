package io.hqew.kquery.logical

import io.hqew.kquery.datasource.CsvDataSource
import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.datatypes.Field
import io.hqew.kquery.datatypes.Schema
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MockLogicalPlan(private val schema: Schema) : LogicalPlan {
    override fun schema(): Schema {
        return schema
    }

    override fun children(): List<LogicalPlan> {
        return listOf()
    }

    override fun toString(): String {
        val b = StringBuilder()
        (0 until schema.fields.size).forEach {
            if (it > 0) {
                b.append(",")
            }
            b.append(schema.fields[it].name)
        }
        return b.toString()
    }
}
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogicalPlanTest {

    @Test
    fun `test format logical plan`() {
        val p = MockLogicalPlan(Schema(listOf(
            Field("1st", ArrowTypes.Int32Type),
            Field("2nd", ArrowTypes.StringType),
        )))
        val formattedPlan = p.pretty()

        print(formattedPlan)
        assertTrue { formattedPlan.isNotEmpty() }
        assertEquals(formattedPlan, "1st,2nd\n")
    }
    @Test
    fun `test format logical plan with csv datasource`() {

        val dir = "../testdata"
        val csvSource = CsvDataSource(
            File(dir, "employee.csv").absolutePath,
            null,
            true,
            1024,
        )

        val plan = Projection(
            Selection(
                Scan("employee.csv", csvSource, listOf()),
                Eq(Column("state"), LiteralString("CO"))
            ),
            listOf(
                Column("id"),
                Column("first_name"),
                Column("last_name"),
            )
        )

        assertEquals(
            format(plan),
            "Projection: #id, #first_name, #last_name\n\tSelection: #state = 'CO'\n\t\tScan: employee.csv; projection=None\n"
        )
    }
}