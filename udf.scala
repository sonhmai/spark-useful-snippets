/**
User Defined Functions

Cons:
- losing all optimization Spark does on Dataframe/ Dataset.
- null hanlding - programmer responsibility to handle null gracefully

References:
- https://medium.com/@achilleus/spark-udfs-we-can-use-them-but-should-we-use-them-2c5a561fde6d

*/

// ==================
// Example of losing PredicatePushdown

// using df operation
df1.where('name === "Joey").queryExecution.executedPlan
// PushedFilters: [IsNotNull(name), EqualTo(name,Joey)]

// using udf
val isJoey = udf((name:String) => name == "Joey")
df.where(isJoey($"name")).queryExecution.executedPlan
// PushedFilters: []

// ============= Vectorized UDF
