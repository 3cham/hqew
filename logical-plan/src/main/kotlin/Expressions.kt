package io.hqew.kquery.logical

import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.datatypes.Field
import java.sql.SQLException

class Column(val name: String) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields.find { it.name == name }
            ?: throw SQLException("No column named $name in ${input.schema().fields.map {it.name}}")
    }

    override fun toString(): String {
        return "#$name"
    }
}

fun col(name: String) = Column(name)

// fun max(expr: LogicalExpr) = Max(expr)


class ColumnIndex(val i: Int) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields[i]
    }

    override fun toString(): String {
        return "#$i"
    }
}


class LiteralString(val str: String) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(str, ArrowTypes.StringType)
    }

    override fun toString(): String {
        return "'$str'"
    }
}

fun lit(value: String) = LiteralString(value)


class LiteralLong(val n: Long): LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(n.toString(), ArrowTypes.Int64Type)
    }

    override fun toString(): String {
        return n.toString()
    }
}

fun lit(value: Long) = LiteralLong(value)