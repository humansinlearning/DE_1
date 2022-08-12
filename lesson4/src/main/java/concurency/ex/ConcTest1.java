package concurency.ex;

import java.util.concurrent.atomic.AtomicInteger;

public class ConcTest1 {
  static int value = 0;

  public static void main(String[] args) throws InterruptedException {
    Object synchronizer = new Object(); // Monitor - synchronization monitor
    long startNanos = System.nanoTime();
    // Thread 1
    Thread t1 = new Thread(() -> {
      for (int i = 0; i < 1000_0000; i++) {
        synchronized (synchronizer) {
          value++;
        }
      }
    });
    Thread t2 = new Thread(() -> {
      for (int i = 0; i < 1000_0000; i++) {
        synchronized (synchronizer) {
          value++;
        }
      }
    });
    t1.start();
    // 2 Thread
    t2.start();
    // 3 Thread
    t1.join();
    t2.join();
    System.out.println(System.nanoTime() - startNanos);
    System.out.println(value);
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
