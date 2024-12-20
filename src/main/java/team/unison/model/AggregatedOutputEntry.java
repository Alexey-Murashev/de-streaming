package team.unison.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class AggregatedOutputEntry implements Serializable {

    private String district;
    private String type;
    private Float averagePrice;
    private Float averageLivingArea;
    private Float averagePricePerSqm;
    private Integer totalAmount;

    public String toString() {
        StringBuilder builder = new StringBuilder();
        return builder
                .append(district)
                .append(",")
                .append(type)
                .append(",")
                .append(averagePrice)
                .append(",")
                .append(averageLivingArea)
                .append(",")
                .append(averagePricePerSqm)
                .append(",")
                .append(totalAmount)
                .toString();
    }
}
