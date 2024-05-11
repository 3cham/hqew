import io.hqew.kquery.physical.expressions.MaxAccumulator
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

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
}