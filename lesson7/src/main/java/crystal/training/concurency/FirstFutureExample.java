package crystal.training.concurency;

import java.util.concurrent.*;

public class FirstFutureExample {
  public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
    ExecutorService service = Executors.newFixedThreadPool(4);
    Future<String> resultHello = service.submit(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Thread.sleep(10);
        return "Hello ";
      }
    });
    Future<String> resultKitty = service.submit(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Thread.sleep(10);
        return "Kitty";
      }
    });
    System.out.println(resultHello.get(1, TimeUnit.SECONDS) + resultKitty.get());

    service.shutdown();
    service.awaitTermination(10, TimeUnit.SECONDS);
    service.shutdownNow();
  }
}
