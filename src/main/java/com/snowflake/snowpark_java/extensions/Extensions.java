package com.snowflake.snowpark_java.extensions;

import com.snowflake.snowpark_java.SessionBuilder;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Row;

import static com.snowflake.snowpark_java.Functions.lit;
import static com.snowflake.snowpark_java.Functions.sqlExpr;
import static com.snowflake.snowpark_java.Functions.col;
import static com.snowflake.snowpark_java.Functions.callUDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.FileReader;
import java.io.BufferedReader;

public class Extensions {


    private static Map<String, String> read_connections(String sectionName) 
    {
        Map<String, String> sectionMap = new HashMap<>();
        String currentSection = "";
        boolean inTargetSection = false;
        String filename = System.getProperty("user.home") + "/.snowflake/connections.toml";
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.startsWith(";") || line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith("[") && line.endsWith("]")) {
                    currentSection = line.substring(1, line.length() - 1);
                    inTargetSection = currentSection.equals(sectionName);
                } else if (!line.isEmpty() && inTargetSection) {
                    String[] keyValue = line.split("=", 2);
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();
                    if (value.startsWith("\""))
                        value = value.substring(1,value.length()-1);
                    switch (key) {
                        case "account":
                        case "accountname":
                            value = "http://" + value + ".snowflakecomputing.com";
                            key = "URL";
                            break;
                        case "user":
                        case "username":
                            key = "USER";
                            break;
                        case "password":
                            key = "PASSWORD";
                            break;
                        case "rolename":
                            key = "ROLE";
                            break;
                        case "database":
                        case "dbname":
                            key = "DATABASE";
                            break;
                        case "schemaname":
                            key = "SCHEMA";
                            break;
                        case "warehousename":
                            key = "WAREHOUSE";
                            break;
                        default:
                            break;
                    }
                    sectionMap.put(key, value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sectionMap;
    }
    

    public static Map<String, DataFrame> aliasMap = new HashMap<>();

    public static DataFrame as(DataFrame df, String alias) {
        aliasMap.put(alias, df);
        return df;
    }

    public static Column as(com.snowflake.snowpark_java.Column thiz, String alias) {
        return thiz.alias(alias);
    }

    public static Column between(com.snowflake.snowpark_java.Column thiz, String lowerBound, String upperBound) {
        return thiz.between(lit(lowerBound), lit(upperBound));
    }

    public static String[] columns(DataFrame df) {
        return fieldNames(df.schema());
    }

    public static String[] fieldNames(StructType struct) {
        var fields = new ArrayList<String>();
        var iterator = struct.iterator();
        while (iterator.hasNext()) {
            var field = iterator.next();
            fields.add(field.name());
        }
        return fields.toArray(new String[fields.size()]);
    }

    public static Column divide(com.snowflake.snowpark_java.Column thiz, int x) {
        return thiz.gt(lit(x));
    }

    public static Column eqNullSafe(com.snowflake.snowpark_java.Column thiz, Column other) {
        return callUDF("EQUAL_NULL", thiz, other);
    }

    public static Column equalTo(com.snowflake.snowpark_java.Column thiz, int other) {
        return thiz.equal_to(lit(other));
    }

    public static Column equalTo(com.snowflake.snowpark_java.Column thiz, double other) {
        return thiz.equal_to(lit(other));
    }

    public static Column equalTo(com.snowflake.snowpark_java.Column thiz, Column other) {
        return thiz.equal_to(other);
    }

    public static Column equalTo(com.snowflake.snowpark_java.Column thiz, String other) {
        return thiz.equal_to(lit(other));
    }

    public static StructField[] fields(StructType struct) {
        var fields = new ArrayList<StructField>();
        var iterator = struct.iterator();
        while (iterator.hasNext()) {
            var field = iterator.next();
            fields.add(field);
        }
        return fields.toArray(new StructField[fields.size()]);
    }
    /**
     * Gets the field index by name.
     * @deprecated
     * Is Snowpark rows do not contains schema information, so this information needs
     * a reference to the source DataFrame.
     * However operating directly on rows is discouraged. Try to leverage DataFrame API as much as possible.
     */
    @Deprecated
    public static int fieldIndex(com.snowflake.snowpark_java.Row the_row, String columnName) {
        throw new RuntimeException("The list of column is needed");
    }

    public static int fieldIndex(com.snowflake.snowpark_java.Row thiz, String[] columns, String colName) {
        for (var i = 0; i < columns.length; i++) {
            if (columns[i] == colName) {
                return i;
            }
        }
        throw new RuntimeException("column " + colName + " not found");
    }


    public static DataFrame filter(DataFrame df, Column filter) {
        return df.filter(filter);
    }

    public static DataFrame filter(DataFrame df, String filter) {
        return df.filter(Functions.sqlExpr(filter));
    }

    public static SessionBuilder from_connection(SessionBuilder builder, String connection_name) {
        var configs = read_connections(connection_name);
        builder.configs(configs);
        return builder;
    }

    public static Column gep(com.snowflake.snowpark_java.Column thiz, int other) {
        return thiz.geq(lit(other));
    }

    public static Column gep(com.snowflake.snowpark_java.Column thiz, Column other) {
        return thiz.geq(other);
    }

    public static Column gt(com.snowflake.snowpark_java.Column thiz, int x) {
        return thiz.gt(lit(x));
    }

    public static Column isNotNull(com.snowflake.snowpark_java.Column thiz) {

        return thiz.is_not_null();
    }

    public static Column isNull(com.snowflake.snowpark_java.Column thiz) {

        return thiz.is_null();
    }

    public static Column isin(com.snowflake.snowpark_java.Column thiz, String... values) {
        // Object[] objectArray = new Object[values.length];
        // for (int i = 0; i < values.length; i++) {
        // objectArray[i] = lit(values[i]);
        // }
        return thiz.in((Object[]) values);
    }

    public static Column like(com.snowflake.snowpark_java.Column thiz, String pattern) {
        return thiz.like(lit(pattern));
    }

    public static Column lt(com.snowflake.snowpark_java.Column thiz, int x) {
        return thiz.lt(lit(x));
    }

    public static Column lt(com.snowflake.snowpark_java.Column thiz, String x) {
        return thiz.lt(lit(x));
    }

    public static Column multiply(com.snowflake.snowpark_java.Column thiz, int x) {
        return thiz.multiply(lit(x));
    }

    public static Column notEqual(com.snowflake.snowpark_java.Column thiz, Column other) {
        return thiz.not_equal(other);
    }

    public static Column notEqual(com.snowflake.snowpark_java.Column thiz, int other) {
        return thiz.not_equal(lit(other));
    }

    public static Column notEqual(com.snowflake.snowpark_java.Column thiz, double other) {
        return thiz.not_equal(lit(other));
    }

    public static Column notEqual(com.snowflake.snowpark_java.Column thiz, String other) {
        return thiz.not_equal(lit(other));
    }

    public static Column not_equal(com.snowflake.snowpark_java.Column thiz, double other) {
        return thiz.not_equal(lit(other));
    }

    public static Column otherwise(com.snowflake.snowpark_java.CaseExpr thiz, int x) {
        return thiz.otherwise(lit(x));
    }

    public static Column otherwise(com.snowflake.snowpark_java.CaseExpr thiz, Column x) {
        return thiz.otherwise(x);
    }

    /**
     * Equivalent to Spark's selectExpr. Selects columns based on the expressions
     * specified. They could either be
     * column names, or calls to other functions such as conversions, case
     * expressions, among others.
     *
     * @param exprs Expressions to apply to select from the DataFrame.
     * @return DataFrame with the selected expressions as columns. Unspecified
     * columns are not included.
     */
    public static DataFrame selectExpr(DataFrame df, String... exprs) {
        List<Column> expressions = new ArrayList<Column>();
        for (var expr : exprs) {
            expressions.add(sqlExpr(expr));
        }
        return df.select(expressions.toArray(new Column[expressions.size()]));
    }
    

    public static Column startsWith(com.snowflake.snowpark_java.Column thiz, String literal) {
        return Functions.startswith(thiz, lit(literal));
    }

    public static Column substr(Column c, int pos, int len) {
        return Functions.substring(c, Functions.lit(pos), Functions.lit(len));
    }

    public static DataFrame withColumnRenamed(DataFrame df, String newName, String oldName) {
        return df.rename(newName, col(oldName));
    }

}