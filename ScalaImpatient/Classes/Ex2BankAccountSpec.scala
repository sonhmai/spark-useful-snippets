// test for bank account exercise 2
import org.scalatest.flatspec.AnyFlatSpec

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

class BankAccountSpec extends AnyFlatSpec {
    "Bank Account" should "increase balance when deposited" in {
        val acc = new BankAccount
        acc.deposit(10.5)
        assert(acc.balance == 10.5)
    }

    it should "decrease balance when withdrew" in {
        val acc = new BankAccount
        acc.deposit(10.3)
        acc.withdraw(9.9)
        assert(acc.balance) == 0.4
    }
}