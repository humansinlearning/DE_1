package crystal.training.de;

import crystal.training.de.api.client.ApiClient;
import crystal.training.de.api.client.ApiException;
import crystal.training.de.api.client.api.PublicHolidayApi;
import crystal.training.de.api.client.model.PublicHolidayV3Dto;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class PublicHolidays {
  @Builder(toBuilder = true)
  @Data
  static class CountryInfo {
    private String name;
    private String code;
    private int numberOfHolidays;
  }

  public static void main(String[] args) throws IOException, ApiException {
    // Common pattern in java IS
    // 1 Open resource (FILE, SOCKET, STREAM)
    // Use it
    // CLOSE IT
    // Res r1 = openRes();
    // try {
    //    r1.xx();
    //    ..
    // } finally {
    //   r1.close();
    // }
    // try (Res r1=openRes()) {
    //  ... use r1
    // }
    InputStream is = PublicHolidays.class.getResourceAsStream("/cc.csv");
    try(CSVParser csvParser = new CSVParser(new InputStreamReader(is, StandardCharsets.UTF_8),CSVFormat.DEFAULT)) {
      List<CountryInfo> countries = csvParser.stream()
          .map(csvRecord -> CountryInfo.builder()
              .name(csvRecord.get(0))
              .code(csvRecord.get(1))
              .build())
          .collect(Collectors.toList());
      System.out.println(countries);
    }

    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath("https://date.nager.at");
    PublicHolidayApi publicHolidayApi = new PublicHolidayApi(apiClient);
    List<PublicHolidayV3Dto> res = publicHolidayApi.publicHolidayPublicHolidaysV3(2022, "AL");
    System.out.println(res);
  }
}
