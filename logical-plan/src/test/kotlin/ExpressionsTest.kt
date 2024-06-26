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

    @Test
    fun `test UnaryExpr & BinaryExpr toString`() {
        val l = lit(10.toLong())
        val r = lit(11.toLong())

        assertEquals(Not(l).toString(), "NOT 10")

        assertEquals(And(l, r).toString(), "10 AND 11")
        assertEquals(Or(l, r).toString(), "10 OR 11")

        assertEquals(Eq(l, r).toString(), "10 = 11")
        assertEquals(l.eq(r).toString(), "10 = 11")

        assertEquals(l.neq(r).toString(), "10 != 11")
        assertEquals(Neq(l, r).toString(), "10 != 11")

        assertEquals(Lt(l, r).toString(), "10 < 11")
        assertEquals(l.lt(r).toString(), "10 < 11")

        assertEquals(LtEq(l, r).toString(), "10 <= 11")
        assertEquals(l.lteq(r).toString(), "10 <= 11")

        assertEquals(Gt(l, r).toString(), "10 > 11")
        assertEquals(l.gt(r).toString(), "10 > 11")

        assertEquals(GtEq(l, r).toString(), "10 >= 11")
        assertEquals(l.gteq(r).toString(), "10 >= 11")
    }

    @Test
    fun `test MathExpr toString`() {
        val l = lit(10.toLong())
        val r = lit(11.toLong())

        assertEquals(Add(l, r).toString(), "10 + 11")
        assertEquals(l.add(r).toString(), "10 + 11")

        assertEquals(Subtract(l, r).toString(), "10 - 11")
        assertEquals(l.subtract(r).toString(), "10 - 11")

        assertEquals(Multiply(l, r).toString(), "10 * 11")
        assertEquals(l.mult(r).toString(), "10 * 11")

        assertEquals(Divide(l, r).toString(), "10 / 11")
        assertEquals(l.div(r).toString(), "10 / 11")

        assertEquals(Modulus(l, r).toString(), "10 % 11")
        assertEquals(l.mod(r).toString(), "10 % 11")

        assertEquals(Add(l, r).alias("sumlr").toString(), "10 + 11 as sumlr")

        assertEquals(Add(l, r).subtract(lit(5.toLong())).alias("expr").toString(), "10 + 11 - 5 as expr")
    }

    @Test
    fun `test AggregateExpr toString`() {
        val col = col("column_A")

        assertEquals(Max(col).toString(), "MAX(#column_A)")
        assertEquals(Min(col).toString(), "MIN(#column_A)")
        assertEquals(Sum(col).toString(), "SUM(#column_A)")
        assertEquals(Avg(col).toString(), "AVG(#column_A)")
        assertEquals(Count(col).toString(), "COUNT(#column_A)")
        assertEquals(CountDistinct(col).toString(), "COUNT(DISTINCT #column_A)")
    }

}