package crystal;

import javax.inject.Inject;
import javax.inject.Named;

public class TransportCompany {

  Car car;

  @Inject
  public TransportCompany(@Named("renault") Car car) {
    this.car = car;
  }


  public void setCar(Car car) {
    this.car = car;
  }

  @Override
  public String toString() {
    return "TransportCompany{" +
        "car=" + car +
        '}';
  }
}
