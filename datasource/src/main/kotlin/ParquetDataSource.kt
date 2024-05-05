package io.hqew.kquery.datasource

import io.hqew.kquery.datatypes.ArrowFieldVector
import io.hqew.kquery.datatypes.RecordBatch
import io.hqew.kquery.datatypes.Schema
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.arrow.schema.SchemaConverter
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types

class ParquetDataSource(
    private val filename: String
) : DataSource {
    override fun schema() : Schema {
        return ParquetScan(filename, listOf()).use {
            val arrowSchema = SchemaConverter().fromParquet(it.schema).arrowSchema
            io.hqew.kquery.datatypes.SchemaConverter.fromArrow(arrowSchema)
        }
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        return ParquetScan(filename, projection)
    }
}

class ParquetScan(filename: String, private val columns: List<String>) : AutoCloseable, Sequence<RecordBatch> {

    private val reader = ParquetFileReader.open(HadoopInputFile.fromPath(Path(filename), Configuration()))
    val schema = reader.footer.fileMetaData.schema
    override fun iterator(): Iterator<RecordBatch> {
        return ParquetIterator(reader, columns)
    }

    override fun close() {
        reader.close()
    }
}

class ParquetIterator(
    private val reader: ParquetFileReader, private val projectedColumns: List<String>
) : Iterator<RecordBatch> {

    val schema = reader.footer.fileMetaData.schema
    val arrowSchema = SchemaConverter().fromParquet(schema).arrowSchema

    val projectedArrowSchema = org.apache.arrow.vector.types.pojo.Schema(
        projectedColumns.map{ name -> arrowSchema.fields.find { field -> field.name == name }}
    )
    private val projectedColumnIndex = projectedColumns.map{ name ->
        arrowSchema.fields.withIndex().find { field -> field.value.name == name }?.index ?: -1
    }

    var batch: RecordBatch? = null
    override fun hasNext(): Boolean {
        batch = nextBatch()
        return batch != null
    }

    override fun next(): RecordBatch {
        val next = batch
        batch = null
        return next!!
    }

    private fun nextBatch(): RecordBatch? {
        val pages = reader.readNextRowGroup() ?: return null

        if (pages.rowCount > Integer.MAX_VALUE) {
            throw IllegalStateException()
        }

        val rows = pages.rowCount.toInt()
        println("Reading $rows rows")

        val root = VectorSchemaRoot.create(projectedArrowSchema, RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        root.rowCount = rows

        // read parquet data from pages into root
        val columnIO = ColumnIOFactory().getColumnIO(schema)
        val recordReader = columnIO.getRecordReader(pages, GroupRecordConverter(schema))

        for (row in 0 until rows) {
            val g = recordReader.read()

            (0 until root.fieldVectors.size).forEach {id ->
                val field = projectedColumnIndex[id]
                val fieldType = g.type.getType(field)
                root.fieldVectors[id].valueCount = rows

                when (val fieldTypeName = fieldType.asPrimitiveType().primitiveTypeName) {
                    null -> throw IllegalStateException("fieldType is null")

                    PrimitiveType.PrimitiveTypeName.INT32 -> {
                        val vector = root.fieldVectors[id] as IntVector
                        vector.setSafe(row, g.getInteger(field, 0))
                    }

                    PrimitiveType.PrimitiveTypeName.INT64 -> {
                        val vector = root.fieldVectors[id] as BigIntVector
                        vector.setSafe(row, g.getLong(field, 0))
                    }
                    PrimitiveType.PrimitiveTypeName.INT96 -> {
                        val vector = root.fieldVectors[id] as VarBinaryVector
                        vector.setSafe(row, g.getValueToString(field, 0).toByteArray())
                    }
                    PrimitiveType.PrimitiveTypeName.BOOLEAN -> {
                        val vector = root.fieldVectors[id] as BitVector
                        if (g.getBoolean(field, 0)) {
                            vector.setSafe(row,1)
                        } else {
                            vector.setSafe(row, 0)
                        }
                    }
                    PrimitiveType.PrimitiveTypeName.BINARY -> {
                        val vector = root.fieldVectors[id] as VarBinaryVector
                        vector.setSafe(row, g.getBinary(field, 0).bytes)
                    }
                    PrimitiveType.PrimitiveTypeName.FLOAT -> {
                        val vector = root.fieldVectors[id] as Float4Vector
                        vector.setSafe(row, g.getFloat(field, 0))
                    }
                    PrimitiveType.PrimitiveTypeName.DOUBLE -> {
                        val vector = root.fieldVectors[id] as Float8Vector
                        vector.setSafe(row, g.getDouble(field, 0))
                    }
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> {
                        val vector = root.fieldVectors[id] as VarCharVector
                        vector.setSafe(row, g.getValueToString(field, 0).toByteArray())
                    }
                    else -> throw UnsupportedOperationException("$fieldTypeName is not supported")
                }
            }
        }

        val hqewSchema = io.hqew.kquery.datatypes.SchemaConverter.fromArrow(projectedArrowSchema)

        return RecordBatch(hqewSchema, root.fieldVectors.map{ ArrowFieldVector(it) })
    }

}
