import io.hqew.kquery.physical.expressions.LiteralLongExpression
import io.hqew.kquery.physical.expressions.MathExpression
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MathExpressionTest {

    fun `test add expression`() {
        val l = LiteralLongExpression(5)
        val r = LiteralLongExpression(4)

        // val rs = MathExpression(l, r).
    }
}