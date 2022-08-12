package concurency.ex;

public class ConcTest2 {
  static class Counter {
    int value = 0;

    synchronized void incValue() {
      value++;
    }
  }

  public static void main(String[] args) throws InterruptedException {
    Counter counter = new Counter();
    long startNanos = System.nanoTime();
    // Thread 1
    Thread t1 = new Thread(() -> {
      for (int i = 0; i < 1000_0000; i++) {
        counter.incValue();
      }
    });
    Thread t2 = new Thread(() -> {
      for (int i = 0; i < 1000_0000; i++) {
        counter.incValue();
      }
    });
    t1.start();
    // 2 Thread
    t2.start();
    // 3 Thread
    t1.join();
    t2.join();
    System.out.println(System.nanoTime() - startNanos);
    System.out.println(counter.value);
    // 126575200  - 10177522
    // 120077700 - 10015965
    // 134354800 - 17268930
  }

  ////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////
  //  T1 value=0       T2  value=0
  //     value=1           value=1
  //     value=2           value=2
  //////  Core cache never flushed -> value=1000_0000

  ///////////////////////////////////////////////////////
  //////////////////////////////////////////////////////
  // T1 value=0 -> 1 - sync cache
  // T2 value=1 -> 2 -> sync cache
  // T1 value=2 -> 3 -> sync cache
  ///////// - value=2000_000
}
