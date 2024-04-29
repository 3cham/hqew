package io.hqew.kquery.datatypes

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LiteralValueVectorTest {

    @Test
    fun `test literal value vector`() {
        val size = 10
        val literalVector = LiteralValueVector(ArrowTypes.Int32Type, 1, size)

        assertEquals(literalVector.getValue(4), 1)
        assertEquals(literalVector.size(), size)
    }
}