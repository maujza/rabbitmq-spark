package com.github.maujza.schema;



import java.io.Serializable;
import java.util.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder$;
import org.apache.spark.sql.types.StructType;

/**
 * A Row to InternalRow function that uses a resolved and bound encoder for the given schema.
 *
 * <p>A concrete {@code Function} implementation that is {@code Serializable}, so it can be
 * serialized and sent to executors.
 */
final class RowToInternalRowFunction implements Function<Row, InternalRow>, Serializable {
    private static final long serialVersionUID = 1L;

    private final ExpressionEncoder.Serializer<Row> serializer;

    RowToInternalRowFunction(final StructType schema) {
        this.serializer = RowEncoder$.MODULE$.apply(schema).createSerializer();
    }

    @Override
    public InternalRow apply(final Row row) {
        return serializer.apply(row);
    }
}
