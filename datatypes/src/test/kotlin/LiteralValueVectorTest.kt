package io.hqew.kquery.datatypes

import kotlin.test.assertEquals
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LiteralValueVectorTest {

  @Test
  fun `test literal value vector`() {
    val size = 10
    val literalVector = LiteralValueVector(ArrowTypes.Int32Type, 1, size)

    assertEquals(literalVector.getValue(4), 1)
    (0 until size).forEach { assertEquals(literalVector.getValue(it), 1) }
    assertEquals(literalVector.size(), size)
  }
}
