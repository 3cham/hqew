import io.hqew.kquery.physical.expressions.MinAccumulator
import io.hqew.kquery.physical.expressions.SumAccumulator
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SumExpressionTest {

    @Test
    fun `sum acc`() {
        val numbers = listOf(1, 2, 3, 5, 4)
        val acc = SumAccumulator()

        numbers.forEach { acc.accumulate(it) }

        assertEquals(15, acc.finalValue())
    }
}