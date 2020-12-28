val explodedDf = df
    .withColumn("slt_ratio_s", explode($"slt_rage"))

val arrayData = Seq(
      Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
    Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
    Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
    Row("Washington",null,null),
    Row("Jefferson",List(),Map())
    )

/**
There are too many sales of product_id "0" in sale table.
Must salt it to avoid sort-merge join skew.

*/

// When a map is passed, it creates two new columns 
// one for key and one for value and each element 
// in map split into the row.

// duplicate product_id 0 rows by using explode
// explode takes another col as arg so need to create new array col.
// For example, explode will generate 5 rows for product_id 0 below
// ---------
// product_id   tmp
//      0       [0,1,2,3,4,5]
//      1       null
val explodedDf = df.explode($"tmp")


// explode does not change col values. what to do?
// Another withColumn with udf to change value of product_id col,
// udf add salted key of col value == "0"
import org.apache.spark.sql.functions.udf

import scala.util.Random

val rnd = new Random
def salt_product_id(product_id: String): String = {
    if (product_id == "0") {
        product_id + "_" + rnd.nextInt(5).toString
    }
    else product_id
}
val salting_udf = udf(salt_product_id)
val saltedDF = explodedDf
    .withColumn("product_id_salted", salting_udf($"product_id"))
    

