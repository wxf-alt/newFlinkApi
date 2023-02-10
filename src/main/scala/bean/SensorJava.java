package bean;

/**
 * @Auther: wxf
 * @Date: 2022/11/2 19:31:03
 * @Description: SensorJava
 * @Version 1.0.0
 */
public class SensorJava {
    private String id;
    private Long timeStamp;
    private Double temperature;

    public SensorJava(String id, Long timeStamp, Double temperature) {
        this.id = id;
        this.timeStamp = timeStamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }
}