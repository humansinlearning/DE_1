package crystal.training;

// lombok
// autovalue from google
public class Person {
  public static class Address {
    private final String country;
    private final String city;


    public Address(String country, String city) {
      this.country = country;
      this.city = city;
    }

    public String getCountry() {
      return country;
    }

    public String getCity() {
      return city;
    }

    public static Builder builder() {
      return new Builder();
    }

    public Builder toBuilder() {
      return new Builder().city(city).country(country);
    }

    private static class Builder {
      private String country;
      private String city;

      public Builder country(String country) {
        this.country = country;
        return this;
      }

      public Builder city(String city) {
        this.city = city;
        return this;
      }

      public Address build() {
        return new Address(country, city);
      }
    }
  }

  private String name;
  private int age;
  private Address address;

  private static class Builder {

    public Person build() {
      return null;
    }
  }

  public static void main(String[] args) {
    Address add1 = new Address("London", "GB");
    Address add2 = Address.builder().country("London")
        .city("GB").build();
    Address add3 = add2.toBuilder()
        .city("Birmingham")
        ////
        .build();
    // TO BE COMPLETED BY YOU
  }
}
