package io.hqew.kquery.logical

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
}