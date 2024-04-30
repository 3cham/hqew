package io.hqew.kquery.logical

import kotlin.test.assertEquals
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ColumnTest {

    @Test
    fun `test toString`() {
        val col = col("testColumn")

        assertEquals(col.toString(), "#testColumn")
    }
}