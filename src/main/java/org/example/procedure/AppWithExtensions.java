package org.example.procedure;

import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.types.*;

import lombok.experimental.ExtensionMethod;
import com.snowflake.snowpark_java.extensions.*;
import static com.snowflake.snowpark_java.Functions.col;
import static com.snowflake.snowpark_java.Functions.round;

@ExtensionMethod ({Extensions.class})
public class AppWithExtensions {

    public static Long run(Session session) {

        // Create schema manually

        StructType schema =
            StructType.create(
                new StructField("customer_id", DataTypes.IntegerType),
                new StructField("first_name", DataTypes.StringType),
                new StructField("last_name", DataTypes.StringType),
                new StructField("status", DataTypes.StringType),
                new StructField("credit_limit", DataTypes.DoubleType)
                );


        // Create rows
        Row[] rows = new Row[] {
        
                Row.create(1, "Alice", "Smith", "ACTIVE", 10000.0),
                Row.create(2, "Bob", "Brown", "INACTIVE", 5000.0),
                Row.create(3, "Carol", "White", "ACTIVE", 12000.0)
        };

        DataFrame df = session.createDataFrame(
            rows,schema
        );

        df.show();

        // Filter: status = 'ACTIVE'
        DataFrame active = df.filter(col("status").equalTo("ACTIVE"));

        // Filter: credit_limit > 10000
        DataFrame highCredit = active.filter(col("credit_limit").gt(10000));

        // Rename customer_id -> customer_id_renamed
        DataFrame renamed = highCredit.withColumnRenamed("customer_id", "customer_id_renamed");

        // Select with transformations
        DataFrame transformed = renamed.selectExpr(
                "customer_id_renamed",
                "UPPER(first_name) AS upper_first_name",
                "last_name",
                "credit_limit * 1.1 AS adjusted_credit"
        );

        // Final filter: adjusted_credit = 13200
        DataFrame result = transformed.filter(col("adjusted_credit").equalTo(13200.0));

        // Show final result
        result.show();

        // For example for
        // //map
        // JavaRDD<Integer> mappedRDD = javaRDD.map(val -> (int)Math.round(val));
        //  
        DataFrame mapped_df = df.map(val-> val.select(round(col("$0"))));
        mapped_df.show();
        return df.count();
    }


    public static void main(String[] args) {
        Session session = Session.builder().from_connection("default").getOrCreate();

        App.run(session);
    }
}
