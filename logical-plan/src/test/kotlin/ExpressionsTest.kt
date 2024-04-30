package io.hqew.kquery.logical

import io.hqew.kquery.datatypes.ArrowTypes
import org.apache.arrow.vector.types.pojo.ArrowType
import kotlin.test.assertEquals
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExpressionsTest {

    @Test
    fun `test Column toString`() {
        val col = col("testColumn")

        assertEquals(col.toString(), "#testColumn")
    }

    @Test
    fun `test LiteralString toString`() {
        val litStr = lit("namedColumn")

        assertEquals(litStr.toString(), "'namedColumn'")
    }

    @Test
    fun `test LiteralLong toString`() {
        val litStr = lit(1e10.toLong())

        assertEquals(litStr.toString(), "10000000000")
    }

    @Test
    fun `test LiteralFloat toString`() {
        val litStr = lit(1e10.toFloat())

        assertEquals(litStr.toString(), "1.0E10")
    }

    @Test
    fun `test LiteralDouble toString`() {
        val litStr = lit(1e10)

        assertEquals(litStr.toString(), "1.0E10")
    }

    @Test
    fun `test CastExpr toString`() {
        val litCast = cast(
            lit(10), ArrowTypes.FloatType
        )

        assertEquals(litCast.toString(), "CAST(10 AS FloatingPoint(SINGLE))")
    }
}