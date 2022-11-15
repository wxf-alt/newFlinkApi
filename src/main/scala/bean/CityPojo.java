package bean;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @Auther: wxf
 * @Date: 2022/11/3 11:00:55
 * @Description: CityPojo
 * @Version 1.0.0
 */
@JsonPropertyOrder({"city", "iso2"})
public class CityPojo {
    public String city;
    public String iso2;

    @Override
    public String toString() {
        return "city：" + city + "\t" + "iso2：" + iso2;
    }
}