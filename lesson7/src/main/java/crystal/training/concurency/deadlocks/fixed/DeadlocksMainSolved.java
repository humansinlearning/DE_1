package crystal.training.concurency.deadlocks.fixed;

public class DeadlocksMainSolved {
  public static void main(String[] args) {
    // we have contend at least 2 critical resources
    BankAccount b1 = new BankAccount("A", 100);
    BankAccount b2 = new BankAccount("B", 50);
    BankTransaction t1 = new BankTransaction(b1, b2, 50);
    BankTransaction t2 = new BankTransaction(b2, b1, 50);


    while (true) {
      new Thread(() -> {
        t1.execute();
        System.out.println(b1);
        System.out.println(b2);
      }).start();
      new Thread(() -> {
        t2.execute();
        System.out.println(b1);
        System.out.println(b2);
      }).start();
    }

  }
}

class BankTransaction {
  private BankAccount debitAccount;
  private BankAccount creditAccount;
  private double amount;

  public BankTransaction(BankAccount debitAccount, BankAccount creditAccount, double amount) {
    this.debitAccount = debitAccount;
    this.creditAccount = creditAccount;
    this.amount = amount;
  }

  public void execute() {
    if (debitAccount.getAccountNo().compareTo(creditAccount.getAccountNo()) >= 0) {
      synchronized (debitAccount) {
        debitAccount.debit(amount);
        try {
          Thread.sleep(2);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        synchronized (creditAccount) {
          creditAccount.credit(amount);
        }
      }
    } else {
      synchronized (creditAccount) {
        creditAccount.credit(amount);
        try {
          Thread.sleep(2);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        synchronized (debitAccount) {
          debitAccount.debit(amount);
        }
      }
    }
  }
}

class BankAccount {
  private String accountNo;
  private double balance;

  public BankAccount(String accountNo, double balance) {
    this.accountNo = accountNo;
    this.balance = balance;
  }

  public String getAccountNo() {
    return accountNo;
  }

  public void setAccountNo(String accountNo) {
    this.accountNo = accountNo;
  }

  public synchronized double getBalance() {
    return balance;
  }

  public void debit(double amount) {
    if (this.balance < amount) {
      throw new IllegalArgumentException("Invalid balance - less then amount required");
    }
    this.balance -= amount;
  }

  public void credit(double amount) {
    this.balance += amount;
  }

  @Override
  public String toString() {
    return "BankAccount{" +
        "accountNo='" + accountNo + '\'' +
        ", balance=" + balance +
        '}';
  }
}
