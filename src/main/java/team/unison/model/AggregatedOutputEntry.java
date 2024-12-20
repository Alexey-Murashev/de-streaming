package team.unison.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class AggregatedOutputEntry implements Serializable {

    private String district;
    private String type;
    private Float averagePrice = (float)0;
    private Float averageLivingArea = (float)0;
    private Float averagePricePerSqm = (float)0;
    private Integer totalAmount = 0;

    public String toString() {
        String builder = district +
                "," +
                type +
                "," +
                averagePrice +
                "," +
                averageLivingArea +
                "," +
                averagePricePerSqm +
                "," +
                totalAmount;
        return builder;
    }

    public AggregatedOutputEntry sum(AggregatedOutputEntry other) {
        this.averagePrice += other.getAveragePrice();
        this.averageLivingArea += other.getAverageLivingArea();
        this.averagePricePerSqm += other.getAveragePricePerSqm();
        this.totalAmount += other.getTotalAmount();
        return this;
    }

    public AggregatedOutputEntry getAverage() {
        AggregatedOutputEntry result = new AggregatedOutputEntry();
        result.setAveragePrice(this.averagePrice / this.totalAmount);
        result.setAverageLivingArea(this.averageLivingArea / this.totalAmount);
        result.setAveragePricePerSqm(this.averagePricePerSqm / this.totalAmount);
        result.setTotalAmount(this.totalAmount);
        return result;
    }
}
