package io.hqew.kquery.datatypes

class RecordBatch(val schema: Schema, val fields: List<ColumnVector>) {
  fun rowCount() = fields.first().size()

  fun columnCount() = fields.size

  fun field(i: Int): ColumnVector {
    return fields[i]
  }

  fun toCSV(): String {
    val b = StringBuilder()
    val columnCount = schema.fields.size

    (0 until rowCount()).forEach { rowIndex ->
      (0 until columnCount).forEach { columnIndex ->
        if (columnIndex > 0) {
          b.append(",")
        }

        val v = fields[columnIndex]
        when (val value = v.getValue(rowIndex)
        ) {
          null -> b.append("null")
          is ByteArray -> b.append(String(value))
          else -> b.append(value)
        }
      }
      b.append("\n")
    }

    return b.toString()
  }

  override fun toString(): String {
    return toCSV()
  }
}
