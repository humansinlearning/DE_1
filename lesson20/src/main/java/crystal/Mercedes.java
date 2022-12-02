package crystal;

import javax.inject.Inject;

public class Mercedes implements Car {
  Fuel fuel;

  @Inject
  public Mercedes(Fuel fuel) {
    this.fuel = fuel;
  }
}
