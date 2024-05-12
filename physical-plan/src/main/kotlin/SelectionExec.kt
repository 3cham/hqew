package io.hqew.kquery.physical

import io.hqew.kquery.datatypes.ArrowFieldVector
import io.hqew.kquery.datatypes.ArrowVectorBuilder
import io.hqew.kquery.datatypes.ColumnVector
import io.hqew.kquery.datatypes.FieldVectorFactory
import io.hqew.kquery.datatypes.RecordBatch
import io.hqew.kquery.datatypes.Schema
import io.hqew.kquery.physical.expressions.Expression
import org.apache.arrow.vector.BitVector

class SelectionExec(
    val input: PhysicalPlan,
    val expr: Expression
) : PhysicalPlan {
    override fun execute(): Sequence<RecordBatch> {
        val batches = input.execute()

        return batches.map { batch ->
            // schema should stay the same
            val schema = input.schema()
            val columnCount = batch.columnCount()

            // calculate condition on each row of each batch
            val selected = (expr.evaluate(batch) as ArrowFieldVector).field as BitVector

            val filteredRows = (0 until columnCount).map { col ->
                filter(batch.field(col), selected)
            }
            RecordBatch(schema, filteredRows)
        }
    }

    private fun filter(field: ColumnVector, isSelected: BitVector): ColumnVector {
        val fvf = FieldVectorFactory.create(field.getType(), field.size())

        val av = ArrowVectorBuilder(fvf)
        var count = 0

        (0 until isSelected.valueCount).forEach {
            if (isSelected.get(it) == 1) {
                av.set(count, field.getValue(it))
                count += 1
            }
        }
        av.setValueCount(count)

        return av.build()
    }

    override fun schema(): Schema {
        return input.schema()
    }

    override fun children(): List<PhysicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Filter: $expr"
    }
}