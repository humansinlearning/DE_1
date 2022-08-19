package crystal.training.concurency;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExecutorExample {
  public static void main(String[] args) throws InterruptedException {
    ExecutorService service = Executors.newFixedThreadPool(4);
    for(int i=0;i < 100;i++) {
      service.submit(() -> System.out.println("Hello from " + Thread.currentThread().getName()));
    }
    service.shutdown();
    service.awaitTermination(10, TimeUnit.SECONDS);
    service.shutdownNow();
  }
}