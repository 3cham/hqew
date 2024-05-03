package io.hqew.kquery.datasource

import io.hqew.kquery.datatypes.ArrowFieldVector
import io.hqew.kquery.datatypes.RecordBatch
import io.hqew.kquery.datatypes.Schema
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.arrow.schema.SchemaConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

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

        val hqewSchema = io.hqew.kquery.datatypes.SchemaConverter.fromArrow(projectedArrowSchema)

        return RecordBatch(hqewSchema, root.fieldVectors.map{ ArrowFieldVector(it) })
    }

}
