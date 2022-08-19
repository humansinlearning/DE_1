package crystal.training.concurency.deadlocks.fixed;

// Change the
public class DiningPhilosophers {
  public static void main(String[] args) throws Exception {

    Philosopher[] philosophers = new Philosopher[5];
    Object[] forks = new Object[philosophers.length];

    for (int i = 0; i < forks.length; i++) {
      forks[i] = new Object();
    }

    for (int i = 0; i < philosophers.length; i++) {

      Object leftFork = forks[i];
      Object rightFork = forks[(i + 1) % forks.length];


      if (i == philosophers.length - 1) {
        philosophers[i] = new Philosopher(rightFork, leftFork); // The last philosopher picks up the right fork first
      } else {
        philosophers[i] = new Philosopher(leftFork, rightFork);
      }

      Thread t = new Thread(philosophers[i], "Philosopher " + (i + 1));
      t.start();
    }
  }
}

class Philosopher implements Runnable {

  private final Object leftFork;
  private final Object rightFork;

  Philosopher(Object left, Object right) {
    this.leftFork = left;
    this.rightFork = right;
  }

  private void doAction(String action) throws InterruptedException {
    System.out.println(Thread.currentThread().getName() + " " + action);
    Thread.sleep(((int) (Math.random() * 100)));
  }


  @Override
  public void run() {
    try {
      while (true) {
        doAction(System.nanoTime() + ": Thinking"); // thinking
        // CHANGE THIS ONE SAME WAY AS THE BankTransaction
        // HINT - leftFork and rightFork - not objects anymore -
        // class Fork { int tablePos = 0,1,2,3,4}
        synchronized (leftFork) {
          doAction(System.nanoTime() + ": Picked up left fork");
          synchronized (rightFork) {
            doAction(System.nanoTime() + ": Picked up right fork - eating"); // eating
            doAction(System.nanoTime() + ": Put down right fork");
          }
          doAction(System.nanoTime() + ": Put down left fork. Returning to thinking");
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
