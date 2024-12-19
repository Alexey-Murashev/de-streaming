package team.unison.model;

import com.opencsv.bean.CsvBindByName;
import lombok.Data;

import java.io.Serializable;

@Data
public class CsvDataEntry implements Serializable {

    @CsvBindByName(column = "Price")
    private Float price;
    @CsvBindByName(column = "District")
    private String district;
    @CsvBindByName(column = "City")
    private String city;
    @CsvBindByName(column = "Town")
    private String town;
    @CsvBindByName(column = "Type")
    private String type;
    @CsvBindByName(column = "EnergyCertificate")
    private String energyCertificate;
    @CsvBindByName(column = "Floor")
    private String floor;
    @CsvBindByName(column = "Parking")
    private Float parking;
    @CsvBindByName(column = "HasParking")
    private Boolean hasParking;
    @CsvBindByName(column = "ConstructionYear")
    private Float constructionYear;
    @CsvBindByName(column = "TotalArea")
    private Float totalArea;
    @CsvBindByName(column = "GrossArea")
    private Float grossArea;
    @CsvBindByName(column = "EnergyEfficiency")
    private String energyEfficiency;
    @CsvBindByName(column = "PublishDate")
    private String publishDate;
    @CsvBindByName(column = "Garage")
    private String garage;
    @CsvBindByName(column = "Elevator")
    private String elevator;
    @CsvBindByName(column = "ElectricCarCharger")
    private String electricCarCharger;
    @CsvBindByName(column = "TotalRooms")
    private Float totalRooms;
    @CsvBindByName(column = "NumberOfBedrooms")
    private Float numberOfBedrooms;
    @CsvBindByName(column = "NumberOfWC")
    private Float numberOfWc;
    @CsvBindByName(column = "ConservationStatus")
    private String conservationStatus;
    @CsvBindByName(column = "LivingArea")
    private Float livingArea;
    @CsvBindByName(column = "LotSize")
    private String lotSize;
    @CsvBindByName(column = "BuiltArea")
    private String builtArea;
    @CsvBindByName(column = "NumberOfBathrooms")
    private Float numberOfBathrooms;

}
