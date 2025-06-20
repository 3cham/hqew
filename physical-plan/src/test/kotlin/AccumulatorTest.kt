import io.hqew.kquery.physical.expressions.MaxAccumulator
import io.hqew.kquery.physical.expressions.MinAccumulator
import kotlin.test.assertEquals
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AccumulatorTest {

    @Test
    fun `max accumulator should accumulate value`() {
        val numbers = listOf(1, 2, 3, 5, 4)
        val maxAcc = MaxAccumulator()

        numbers.forEach { maxAcc.accumulate(it) }

        assertEquals(5, maxAcc.finalValue())
    }

    @Test
    fun `max accumulator should accumulate string value`() {
        val numbers = listOf("10", "2", "3", "5", "4", "a")
        val maxAcc = MaxAccumulator()

        numbers.forEach { maxAcc.accumulate(it) }

        assertEquals("a", maxAcc.finalValue())
    }

    @Test
    fun `min accumulator should accumulate value`() {
        val numbers = listOf(1, 2, 3, 5, 4)
        val acc = MinAccumulator()

        numbers.forEach { acc.accumulate(it) }

        assertEquals(1, acc.finalValue())
    }

    @Test
    fun `min accumulator should accumulate string value`() {
        val numbers = listOf("10", "2", "3", "5", "4", "a")
        val acc = MinAccumulator()

        numbers.forEach { acc.accumulate(it) }

        assertEquals("10", acc.finalValue())
    }
}
