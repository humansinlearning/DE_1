package crystal;

public class PiApprox {
  public static void main(String[] args) {
    int insideCircle = 0;
    int insideRect = 0;
    for(int i=0;i < 100_000_000;i++) {
      double x = -1 + 2*Math.random(); // [-1,+1)
      double y = -1 + 2*Math.random();
      if(x*x + y*y <= 1) {
        insideCircle++;
      }
      insideRect++;
    }
    System.out.println("Approx PI=" + (insideCircle*4.0)/insideRect);
    // TO DO
     // implement it functional style - streaming/ parallel streaming
  }
}
