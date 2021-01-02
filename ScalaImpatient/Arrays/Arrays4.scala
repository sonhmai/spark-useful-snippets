// 4. Array of ints, produce new array contains all positive values
// in their original order, followed by all values <= 0 in orig. order.
val a = Array(...)
val posIndices = for (i <- a.indices if a(i)>0) yield i
val notPosIndices = for (i <- a.indices) if a(i) <= 0) yield i
val newArr = new Array[Int](a.length)
for (i <- posIndices.indices) newArr(i) = a(posIndices(i))
for (i <- notPosIndices.indices) newArr(i+posIndices.length) = a(notPosIndices(i))