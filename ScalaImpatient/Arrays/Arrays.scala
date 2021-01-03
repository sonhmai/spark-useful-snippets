// Chap 3: Working with Arrays

// 1. sets a to an array of n random integers between 0 (incl) and n(excl)
r = scala.util.Random
n = 10
val a = (for (i <- 1 to n) yield r.nextInt(n)).toArray
// or
val a = new Array[Int](n) // array of n integers, all init to 0
for (i <- a.indices) {
    a(i) = r.nextInt(n)
}

// 2. A loop swapping adjacent elements of an array of integers in-place
// Example Array(1,2,3,4,5) -> Array(2,1,4,3,5)
val arr = Array(1,2,3,4,5)
var i, temp = 0
while (i < arr.length && i+1 < arr.length) {
    temp = arr(i)
    arr(i) = arr(i+1)
    arr(i+1) = temp
    i = i + 2;
}

// 3. Repeat ex2 use for/yield and produce a new array

// 4. Ex4 has its own file

// 5. Avg of Array[Double]
val arrD = new Array[Double](10)
println(arrD.sum / arrD.length) // sum: Double so result is not rounded to Int

// 6. Not quite understand 

// 7. Produce all values from array with duplicated removed
val dedup = a.distinct()

// 8. ArrayBuffer[Int], remove all but first neg number
// Rewrite the inefficient code below by remove from the end to avoid
// shifting all the elements when removing from the beginning.
var first = true
var n = a.length
var i = 0
while (i<n) {
    if (a(i)>=0) i+=1
    else {
        if (first) {first=false; i+=1}
        else { a.remove(i); n -= 1 }
    }
}
// 8. Refactor
val negIndices = (for (i <- a.indices if a(i)<0) yield i).tail.reverse
for (i <- negIndices) a.remove(i)

// 9. Need more thinking

// 10. related  to Java, deal with later.



