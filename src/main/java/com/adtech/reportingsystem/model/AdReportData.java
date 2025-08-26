package com.adtech.reportingsystem.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDate;

@Entity
@Table(
        name = "ad_report_data",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "unique_ad_report_record",
                        columnNames = {"date", "mobileAppResolvedId", "mobileAppName", "adUnitName", "adUnitId",
                                "inventoryFormatName", "domain", "operatingSystemVersionName",
                                "operatingSystemName", "countryName", "countryCriteriaId"}
                )
        },
        indexes = {
                @Index(name = "idx_ad_report_data_date", columnList = "date"),
                @Index(name = "idx_ad_report_data_mobile_app_name", columnList = "mobileAppName"),
                @Index(name = "idx_ad_report_data_inventory_format", columnList = "inventoryFormatName"),
                @Index(name = "idx_ad_report_data_os_version", columnList = "operatingSystemVersionName"),
                @Index(name = "idx_mobileapp_date", columnList = "mobileAppResolvedId, date")
        }
)
@NoArgsConstructor
@AllArgsConstructor
public class AdReportData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String mobileAppResolvedId;
    private String mobileAppName;
    private String domain;
    private String adUnitName;
    private String adUnitId;
    private String inventoryFormatName;
    private String operatingSystemVersionName;
    private String operatingSystemName;
    private String countryName;
    private String countryCriteriaId;
    private LocalDate date;

    private Long adExchangeTotalRequests;
    private Long adExchangeResponsesServed;
    private Double adExchangeMatchRate;
    private Long adExchangeLineItemLevelImpressions;
    private Long adExchangeLineItemLevelClicks;
    private Double adExchangeLineItemLevelCtr;
    private Double averageEcpm;
    private Double payout;
    private Double adExchangeCostPerClick;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getMobileAppResolvedId() { return mobileAppResolvedId; }
    public void setMobileAppResolvedId(String mobileAppResolvedId) { this.mobileAppResolvedId = mobileAppResolvedId; }

    public String getMobileAppName() { return mobileAppName; }
    public void setMobileAppName(String mobileAppName) { this.mobileAppName = mobileAppName; }

    public String getDomain() { return domain; }
    public void setDomain(String domain) { this.domain = domain; }

    public String getAdUnitName() { return adUnitName; }
    public void setAdUnitName(String adUnitName) { this.adUnitName = adUnitName; }

    public String getAdUnitId() { return adUnitId; }
    public void setAdUnitId(String adUnitId) { this.adUnitId = adUnitId; }

    public String getInventoryFormatName() { return inventoryFormatName; }
    public void setInventoryFormatName(String inventoryFormatName) { this.inventoryFormatName = inventoryFormatName; }

    public String getOperatingSystemVersionName() { return operatingSystemVersionName; }
    public void setOperatingSystemVersionName(String operatingSystemVersionName) { this.operatingSystemVersionName = operatingSystemVersionName; }

    public String getOperatingSystemName() { return operatingSystemName; }
    public void setOperatingSystemName(String operatingSystemName) { this.operatingSystemName = operatingSystemName; }

    public String getCountryName() { return countryName; }
    public void setCountryName(String countryName) { this.countryName = countryName; }

    public String getCountryCriteriaId() { return countryCriteriaId; }
    public void setCountryCriteriaId(String countryCriteriaId) { this.countryCriteriaId = countryCriteriaId; }

    public LocalDate getDate() { return date; }
    public void setDate(LocalDate date) { this.date = date; }

    public Long getAdExchangeTotalRequests() { return adExchangeTotalRequests; }
    public void setAdExchangeTotalRequests(Long adExchangeTotalRequests) { this.adExchangeTotalRequests = adExchangeTotalRequests; }

    public Long getAdExchangeResponsesServed() { return adExchangeResponsesServed; }
    public void setAdExchangeResponsesServed(Long adExchangeResponsesServed) { this.adExchangeResponsesServed = adExchangeResponsesServed; }

    public Double getAdExchangeMatchRate() { return adExchangeMatchRate; }
    public void setAdExchangeMatchRate(Double adExchangeMatchRate) { this.adExchangeMatchRate = adExchangeMatchRate; }

    public Long getAdExchangeLineItemLevelImpressions() { return adExchangeLineItemLevelImpressions; }
    public void setAdExchangeLineItemLevelImpressions(Long adExchangeLineItemLevelImpressions) { this.adExchangeLineItemLevelImpressions = adExchangeLineItemLevelImpressions; }

    public Long getAdExchangeLineItemLevelClicks() { return adExchangeLineItemLevelClicks; }
    public void setAdExchangeLineItemLevelClicks(Long adExchangeLineItemLevelClicks) { this.adExchangeLineItemLevelClicks = adExchangeLineItemLevelClicks; }

    public Double getAdExchangeLineItemLevelCtr() { return adExchangeLineItemLevelCtr; }
    public void setAdExchangeLineItemLevelCtr(Double adExchangeLineItemLevelCtr) { this.adExchangeLineItemLevelCtr = adExchangeLineItemLevelCtr; }

    public Double getAverageEcpm() { return averageEcpm; }
    public void setAverageEcpm(Double averageEcpm) { this.averageEcpm = averageEcpm; }

    public Double getPayout() { return payout; }
    public void setPayout(Double payout) { this.payout = payout; }

    public Double getAdExchangeCostPerClick() { return adExchangeCostPerClick; }
    public void setAdExchangeCostPerClick(Double adExchangeCostPerClick) { this.adExchangeCostPerClick = adExchangeCostPerClick; }
}