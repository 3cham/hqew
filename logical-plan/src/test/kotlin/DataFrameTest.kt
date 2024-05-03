package io.hqew.kquery.logical

import io.hqew.kquery.datasource.CsvDataSource
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataFrameTest {

    @Test
    fun `test DataFrame build`() {
        val df = csv().project(listOf(Column("id")))

        assertEquals(df.schema().fields.size, 1)
        assertEquals(df.logicalPlan().toString(), "Projection: #id")
    }

    @Test
    fun `test DataFrame filter`() {
        val df = csv()
            .filter(Column("first_name").eq(LiteralString("John")))
            .project(listOf(Column("id")))

        assertEquals(df.schema().fields.size, 1)
        assertEquals(format(df.logicalPlan()), "Projection: #id\n\tSelection: #first_name = 'John'\n\t\tScan: employee.csv; projection=None\n")
    }

    @Test
    fun `multiplier and alias`() {

        val df =
            csv()
                .filter(col("state") eq lit("CO"))
                .project(
                    listOf(
                        col("id"),
                        col("first_name"),
                        col("last_name"),
                        col("salary"),
                        (col("salary") mult lit(0.1)) alias "bonus"))
                .filter(col("bonus") gt lit(1000))

        val expected =
            "Selection: #bonus > 1000\n" +
                    "\tProjection: #id, #first_name, #last_name, #salary, #salary * 0.1 as bonus\n" +
                    "\t\tSelection: #state = 'CO'\n" +
                    "\t\t\tScan: employee.csv; projection=None\n"

        val actual = format(df.logicalPlan())

        assertEquals(expected, actual)
    }

    @Test
    fun `aggregate query`() {

        val df =
            csv()
                .aggregate(
                    listOf(col("state")),
                    listOf(Min(col("salary")), Max(col("salary")), Count(col("salary"))))

        assertEquals(
            "Aggregate: groupExpr=[#state], aggregateExpr=[MIN(#salary), MAX(#salary), COUNT(#salary)]\n" +
                    "\tScan: employee.csv; projection=None\n",
            format(df.logicalPlan()))
    }

    private fun csv(): DataFrame {
        val csvSource = CsvDataSource(
            File("../testdata", "employee.csv").absolutePath,
            schema = null,
            hasHeaders = true,
            batchSize = 1024,
        )

        return DataFrameImpl(Scan("employee.csv", csvSource, listOf()))
    }
}