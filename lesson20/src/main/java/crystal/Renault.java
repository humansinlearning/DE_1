package crystal;

import javax.inject.Inject;

public class Renault implements Car {
  Fuel fuel;

  @Inject
  public Renault(Fuel fuel) {
    this.fuel = fuel;
  }
}
