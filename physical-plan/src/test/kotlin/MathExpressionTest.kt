import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.physical.expressions.AddExpression
import io.hqew.kquery.physical.expressions.DivideExpression
import io.hqew.kquery.physical.expressions.LiteralLongExpression
import io.hqew.kquery.physical.expressions.MathExpression
import io.hqew.kquery.physical.expressions.MultiplyExpression
import io.hqew.kquery.physical.expressions.SubtractExpression
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MathExpressionTest {

    @Test
    fun `test add expression`() {
        val l = LiteralLongExpression(5)
        val r = LiteralLongExpression(4)

        val rs = AddExpression(l, r).evaluate(4, 5, ArrowTypes.Int32Type)

        assertEquals(9, rs)
    }

    @Test
    fun `test substract expression`() {
        val l = LiteralLongExpression(5)
        val r = LiteralLongExpression(4)

        val rs = SubtractExpression(l, r).evaluate(4, 5, ArrowTypes.Int32Type)

        assertEquals(-1, rs)
    }

    @Test
    fun `test multiply expression`() {
        val l = LiteralLongExpression(5)
        val r = LiteralLongExpression(4)

        val rs = MultiplyExpression(l, r).evaluate(4, 5, ArrowTypes.Int32Type)

        assertEquals(20, rs)
    }

    @Test
    fun `test divide expression`() {
        val l = LiteralLongExpression(5)
        val r = LiteralLongExpression(4)

        val rs = DivideExpression(l, r).evaluate(4, 5, ArrowTypes.Int32Type)

        assertEquals(0, rs)

        val rsf = DivideExpression(l, r).evaluate(4.0, 5.0, ArrowTypes.DoubleType)

        assertEquals(0.8, rsf)
    }
}