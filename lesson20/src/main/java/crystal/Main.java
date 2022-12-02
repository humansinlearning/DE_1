package crystal;

import crystal.injection.DaggerTransportCompanyComponent;

public class Main {


  public static void main(String[] args) {
//    System.out.println("Hello world!");
//    TransportCompany transportCompany = new TransportCompany(new Renault(new Diesel()));
    // give me a transport company with mercedes vehicle and diesel fuel

    // Inversion of control - patterns
    // Dependency injection - functionality

    // Avoid memory leaks
    // SCOPE - Android - ActivityScope, WebApps - SessionScope, HTTPRequestScope
    // GlobalScope, ApplicationScope
    TransportCompany transportCompany = DaggerTransportCompanyComponent.create().transportCompany();
    Car car = DaggerTransportCompanyComponent.create().getMercedesCar();
    System.out.println();
  }
}