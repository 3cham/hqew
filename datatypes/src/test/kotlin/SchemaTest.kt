package io.hqew.kquery.datatypes

import kotlin.test.assertEquals
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaTest {

  @Test
  fun `test schema`() {
    val allTypes = ArrowTypes.getAll()
    val fields = mutableListOf<Field>()

    // generate a list of all possible ArrowTypes
    allTypes.indices.forEach { fields.add(Field(it.toString(), allTypes[it])) }
    assertEquals(fields.size, allTypes.size)

    val schema = Schema(fields)

    // test column selection for schema
    val selectedFieldIndices = listOf<String>("0", "1", "2")
    val newSchema = schema.select(selectedFieldIndices)
    assertEquals(newSchema.fields.size, selectedFieldIndices.size)
  }

  @Test
  fun `test schema converter`() {
    val allTypes = ArrowTypes.getAll()
    val fields = mutableListOf<Field>()

    // generate a list of all possible ArrowTypes
    allTypes.indices.forEach { fields.add(Field(it.toString(), allTypes[it])) }
    assertEquals(fields.size, allTypes.size)

    val schema = Schema(fields)
    val arrowSchema = schema.toArrow()
    val origSchema = SchemaConverter.fromArrow(arrowSchema)

    (0 until schema.fields.size).forEach {
      assertEquals(schema.fields[it].name, origSchema.fields[it].name)
      assertEquals(schema.fields[it].dataType, origSchema.fields[it].dataType)
    }
  }
}
