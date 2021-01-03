// 1. improve Counter class so it does not return negative at Int.MaxValue
// One way is just to use Long instead of Int
class Counter {
    private var value = 0L
    def increment() { value += 1 }
    def current = value
}
// Another way is to prevent client from incrementing when MaxValue is reached
class Counter {
    private var value = 0
    def increment() {
        if (value < Int.MaxValue) value += 1
        else {
            println(s"Max integer ${Int.MaxValue} reached, cannot increment.")
        }
    }
}

// 2. class BankAccount with methods deposit and withdraw, read-only property balance
// TODO - write tests for this
class BankAccount {
    private var privateBalance: Double = 0.0
    def deposit(amount: Double) { privateBalance += amount }
    def withdraw(amount: Double) {
        if (privateBalance >= amount) {
            privateBalance -= amount
            True // successful
        }
        else False // withdrawal failed
    }
    def balance = privateBalance  // read-only property
}
