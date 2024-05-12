import io.hqew.kquery.datatypes.ArrowTypes
import io.hqew.kquery.datatypes.Field
import io.hqew.kquery.datatypes.Schema
import io.hqew.kquery.fuzzer.Fuzzer
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FuzzerTest {

    @Test
    fun `test fuzzer should generate RecordBatch`() {
        val schema = Schema(listOf(
            Field("0", ArrowTypes.Int64Type),
            Field("1", ArrowTypes.Int64Type)
        ))
        val records = Fuzzer().createRecordBatch(schema, 5)

        assertEquals(records.columnCount(), 2)
        assertEquals(records.rowCount(), 5)
        assertEquals(records.fields.get(0).size(), 5)
    }
}