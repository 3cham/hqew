package io.hqew.kquery.physical

import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.datatypes.Field
import io.hqew.kquery.datatypes.Schema
import io.hqew.kquery.fuzzer.Fuzzer
import io.hqew.kquery.physical.expressions.ColumnExpression
import io.hqew.kquery.physical.expressions.EqExpression
import io.hqew.kquery.physical.expressions.LtEqExpression
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BooleanExpressionTest {

    @Test
    fun `test LtEqExpression with fuzzer`() {
        val schema = Schema(listOf(
            Field("0", ArrowTypes.Int64Type),
            Field("1", ArrowTypes.Int64Type)
        ))

        val l = ColumnExpression(0)
        val r = ColumnExpression(1)

        val input = Fuzzer().createRecordBatch(schema, 5)

        val lteq = LtEqExpression(l, r).evaluate(input)

        (0 until lteq.size()).forEach {
            val expected = input.fields[0].getValue(it) as Long <= input.fields[1].getValue(it) as Long
            assertEquals(expected, lteq.getValue(it))
        }
    }

    @Test
    fun `test EqExpression with string`() {
        val schema = Schema(listOf(
            Field("0", ArrowTypes.StringType),
            Field("1", ArrowTypes.StringType)
        ))

        val l = ColumnExpression(0)
        val r = ColumnExpression(1)

        val values = listOf(
            listOf("aaa", "bbb", "ccc"),
            listOf("aaa", "bbb", "ccd")
        )

        val input = Fuzzer().createRecordBatch(schema, values)

        val eq = EqExpression(l, r).evaluate(input)

        (0 until eq.size()).forEach {
            val expected = input.fields[0].getValue(it) == input.fields[1].getValue(it)
            assertEquals(expected, eq.getValue(it))
        }
    }
}