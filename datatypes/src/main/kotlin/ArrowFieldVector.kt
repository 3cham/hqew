package io.hqew.kquery.datatypes

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.ArrowType

object FieldVectorFactory {
  fun create(arrowType: ArrowType, initialCapacity: Int): FieldVector {
    val rootAllocator = RootAllocator(Long.MAX_VALUE)
    val fieldVector: FieldVector =
        when (arrowType) {
          ArrowTypes.BooleanType -> BitVector("v", rootAllocator)
          ArrowTypes.Int8Type -> TinyIntVector("v", rootAllocator)
          ArrowTypes.Int16Type -> SmallIntVector("v", rootAllocator)
          ArrowTypes.Int32Type -> TinyIntVector("v", rootAllocator)
          ArrowTypes.Int64Type -> BigIntVector("v", rootAllocator)
          ArrowTypes.UInt8Type -> TinyIntVector("v", rootAllocator)
          ArrowTypes.UInt16Type -> SmallIntVector("v", rootAllocator)
          ArrowTypes.UInt32Type -> TinyIntVector("v", rootAllocator)
          ArrowTypes.UInt64Type -> BigIntVector("v", rootAllocator)
          ArrowTypes.FloatType -> Float4Vector("v", rootAllocator)
          ArrowTypes.DoubleType -> Float8Vector("v", rootAllocator)
          ArrowTypes.StringType -> VarCharVector("v", rootAllocator)
          else -> throw IllegalStateException()
        }
    if (initialCapacity != 0) {
      fieldVector.setInitialCapacity(initialCapacity)
    }

    fieldVector.allocateNew()
    return fieldVector
  }
}

class ArrowFieldVector(val field: FieldVector) : ColumnVector {
  override fun getType(): ArrowType {
    return when (field) {
      is BitVector -> ArrowTypes.BooleanType
      is TinyIntVector -> ArrowTypes.Int8Type
      is SmallIntVector -> ArrowTypes.Int16Type
      is IntVector -> ArrowTypes.Int32Type
      is BigIntVector -> ArrowTypes.Int64Type
      is Float4Vector -> ArrowTypes.FloatType
      is Float8Vector -> ArrowTypes.DoubleType
      is VarCharVector -> ArrowTypes.StringType
      else -> throw IllegalStateException()
    }
  }

  override fun getValue(i: Int): Any? {
    if (field.isNull(i)) {
      return null
    }
    return when (field) {
      is BitVector -> field.get(i) == 1
      is TinyIntVector -> field.get(i)
      is SmallIntVector -> field.get(i)
      is IntVector -> field.get(i)
      is BigIntVector -> field.get(i)
      is Float4Vector -> field.get(i)
      is Float8Vector -> field.get(i)
      is VarCharVector -> {
        when (val bytes = field.get(i)
        ) {
          null -> null
          else -> String(bytes)
        }
      }
      is VarBinaryVector -> {
        when (val bytes = field.get(i)
        ) {
          null -> null
          else -> String(bytes)
        }
      }
      else -> throw IllegalStateException("${field.field.fieldType.type} is not supported")
    }
  }

  override fun size(): Int {
    return field.valueCount
  }
}
