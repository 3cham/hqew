package io.hqew.kquery.datatypes

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordBatchTest {

    @Test
    fun `test record batch`() {
        val allTypes = ArrowTypes.getAll()
        val fields = mutableListOf<Field>()

        // generate a list of all possible ArrowTypes
        allTypes.indices.forEach{ fields.add(Field(it.toString(), allTypes[it])) }
        assertEquals(fields.size, allTypes.size)

        val schema = Schema(fields)
        val arrowFields = mutableListOf<ColumnVector>()

        (0 until fields.size).forEach {
            val fieldVector = FieldVectorFactory.create(fields[it].dataType, 1)
            val arrowVector = ArrowVectorBuilder(fieldVector)
            arrowVector.set(0, 0)
            arrowVector.setValueCount(1)
            arrowFields.add(arrowVector.build())
        }

        val records = RecordBatch(schema, arrowFields)

        assertEquals(records.columnCount(), fields.size)
        assertEquals(records.rowCount(), 1)
    }

    @Test
    fun `test records toCSV`() {
        val allTypes = ArrowTypes.getAll()
        val fields = mutableListOf<Field>()

        // generate a list of all possible ArrowTypes
        allTypes.indices.forEach{ fields.add(Field(it.toString(), allTypes[it])) }
        assertEquals(fields.size, allTypes.size)

        val schema = Schema(fields)
        val arrowFields = mutableListOf<ColumnVector>()

        (0 until fields.size).forEach {
            val fieldVector = FieldVectorFactory.create(fields[it].dataType, 1)
            val arrowVector = ArrowVectorBuilder(fieldVector)
            arrowVector.set(0, 0)
            arrowVector.setValueCount(1)
            arrowFields.add(arrowVector.build())
        }

        val records = RecordBatch(schema, arrowFields)
        val csvString = records.toString()

        assertEquals(csvString, "false,0,0,0,0,0,0,0,0,0.0,0.0,0\n")
    }
}