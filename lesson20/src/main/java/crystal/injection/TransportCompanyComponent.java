package crystal.injection;

import crystal.Car;
import crystal.TransportCompany;
import dagger.Component;

import javax.inject.Named;

@Component(modules = {ConfigModule.class})
public interface TransportCompanyComponent {
  @Component.Builder
  interface Builder {
    TransportCompanyComponent build();
  }

  TransportCompany transportCompany();

  @Named("mercedes")
  Car getMercedesCar();

  @Named("renault")
  Car getRenaultCar();
}
