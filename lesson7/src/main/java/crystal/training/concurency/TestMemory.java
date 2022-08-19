package crystal.training.concurency;

public class TestMemory {
  static int x = 1;
  static int y = 2;

  static volatile int v1 = 1;
  static volatile int v2 = 2;

  public static void main(String[] args) {
    int r = f1(); // int r = 44;
    int w = f2(); // int w = 44 ???

    new Thread(() -> System.out.println("bau"));
    new Thread(new Runnable() {
      @Override
      public void run() {
        System.out.println("bau");
      }
    });
    // BY SUBCLASSING
    new Thread() {
      @Override
      public void run() {
        System.out.println("bau");
        throw new RuntimeException("BAUUU");
      }
    };
    // Thread RUNS BETWEEN calling start() and run method finishes (normal or exceptionally)
  }

  public static int f1() {
    int a = 1;
    int b = 2;
    a = 33;
    b = 44;
    a = b;
    return a;
  }

  public static int f2() {
    x = 1;
    y = 2;
    x = 33;
    y = 44;
    x = y;
    return x;
    /**
     * Main memory   x     y
     *               1    2 ?? POSSIBLE
     *               44   44 ! CERTAIN !!
     *               1    44 ? POSSIBLE
     *               33   2 ? POSSIBLE
     *               44   2 ! POSSIBLE
     */
  }

  public static int f2_reorg() {
    //x = 1;
    x = 33;
    //y = 2;
    y = 44;
    x = y;
    return x;
  }

  public static int f3() {
    v1 = 1;
    v2 = 2;
    v1 = 33;
    v2 = 44;
    v1 = v2;
    return v1;
    /**
     * Main memory   v1   v2
     *               1    2 ?? POSSIBLE
     *               44   44 ! CERTAIN !!
     *               1    44 ? IMPOSSIBLE
     *               33   2 ? POSSIBLE
     *               44   2 ! IMPOSSIBLE
     */
  }

  public static int f3_opt() {
    v1 = 1;
    v1 = 33;
    v2 = 2;
    v2 = 44;
    v1 = v2;
    return v1;
  }
}
