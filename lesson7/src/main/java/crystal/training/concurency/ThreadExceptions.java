package crystal.training.concurency;

public class ThreadExceptions {
  public static void main(String[] args) {
    Thread.UncaughtExceptionHandler oldOne = Thread.getDefaultUncaughtExceptionHandler();
    //System.out.println("old exHandler" + oldOne);
    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        System.out.println("Thread " + t.getName() + " killed by " + e);
        //oldOne.uncaughtException(t, e);
      }
    });
    new Thread(() -> {
      throw new RuntimeException("BAUUU");
    }).start();
    // 2 ISSUES !!!
    // 1. Threads are using MEMORY - STACK!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // 2. Context switch from one thread to another .. large percentage of time burned only to switch thread context ..
    // = DONT USE TOO MANY THREADS - in 1990-2000 - 1000 threads !!! ..
    // Today - 8 cores <-> 8 Threads - 24 Threads .. (50 it's acceptable)
    // 1 CORE CAN PROCESS LIKE 1+ GByte per second
  }
}
