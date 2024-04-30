package io.hqew.kquery.datatypes

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaTest {

    @Test
    fun `test schema`() {
        val allTypes = ArrowTypes.getAll()
        val fields = mutableListOf<Field>()

        // generate a list of all possible ArrowTypes
        allTypes.indices.forEach{ fields.add(Field(it.toString(), allTypes[it])) }
        assertEquals(fields.size, allTypes.size)

        val schema = Schema(fields)

        // test column selection for schema
        val selectedFieldIndices = listOf<String>("0", "1", "2")
        val newSchema = schema.select(selectedFieldIndices)
        assertEquals(newSchema.fields.size, selectedFieldIndices.size)
    }
}