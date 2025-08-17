package com.adtech.reportingsystem.dto;

import java.util.List;

public class ReportRequest {
    private String startDate;
    private String endDate;
    private Integer offset;
    private Integer limit;
    private List<String> groupByDimensions;
    private List<String> metrics;
    private List<String> mobileAppNames;
    private List<String> inventoryFormatNames;
    private List<String> operatingSystemVersionNames;
    private List<String> mobileAppResolvedIds;
    private List<String> domains;
    private List<String> adUnitNames;
    private List<String> adUnitIds;

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public List<String> getGroupByDimensions() {
        return groupByDimensions;
    }

    public void setGroupByDimensions(List<String> groupByDimensions) {
        this.groupByDimensions = groupByDimensions;
    }

    public List<String> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<String> metrics) {
        this.metrics = metrics;
    }

    public List<String> getMobileAppNames() {
        return mobileAppNames;
    }

    public void setMobileAppNames(List<String> mobileAppNames) {
        this.mobileAppNames = mobileAppNames;
    }

    public List<String> getInventoryFormatNames() {
        return inventoryFormatNames;
    }

    public void setInventoryFormatNames(List<String> inventoryFormatNames) {
        this.inventoryFormatNames = inventoryFormatNames;
    }

    public List<String> getOperatingSystemVersionNames() {
        return operatingSystemVersionNames;
    }

    public void setOperatingSystemVersionNames(List<String> operatingSystemVersionNames) {
        this.operatingSystemVersionNames = operatingSystemVersionNames;
    }

    public List<String> getMobileAppResolvedIds() {
        return mobileAppResolvedIds;
    }

    public void setMobileAppResolvedIds(List<String> mobileAppResolvedIds) {
        this.mobileAppResolvedIds = mobileAppResolvedIds;
    }

    public List<String> getDomains() {
        return domains;
    }

    public void setDomains(List<String> domains) {
        this.domains = domains;
    }

    public List<String> getAdUnitNames() {
        return adUnitNames;
    }

    public void setAdUnitNames(List<String> adUnitNames) {
        this.adUnitNames = adUnitNames;
    }

    public List<String> getAdUnitIds() {
        return adUnitIds;
    }

    public void setAdUnitIds(List<String> adUnitIds) {
        this.adUnitIds = adUnitIds;
    }

    @Override
    public String toString() {
        return "ReportRequest{" +
                "startDate='" + startDate + '\'' +
                ", endDate='" + endDate + '\'' +
                ", offset=" + offset +
                ", limit=" + limit +
                ", groupByDimensions=" + groupByDimensions +
                ", metrics=" + metrics +
                ", mobileAppNames=" + mobileAppNames +
                ", inventoryFormatNames=" + inventoryFormatNames +
                ", operatingSystemVersionNames=" + operatingSystemVersionNames +
                ", mobileAppResolvedIds=" + mobileAppResolvedIds +
                ", domains=" + domains +
                ", adUnitNames=" + adUnitNames +
                ", adUnitIds=" + adUnitIds +
                '}';
    }
}