package crystal.injection;

import crystal.*;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;

@Module
public interface ConfigModule {
  @Binds
  @Named("mercedes")
  Car getCarMercedes(Mercedes mercedes);

  @Binds
  @Named("renault")
  Car getCarRenault(Renault renault);


//  @Provides
//  static Fuel getFuel() {
//    return new Diesel();
//  }

  @Binds
  Fuel getFuelInjected(Diesel diesel);
}
