package com.adtech.reportingsystem.repository; // Ensure this package matches your actual repository package

import com.adtech.reportingsystem.model.AdReportData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional; // Important: Ensure this is imported

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Repository // This annotation is crucial for Spring to detect it as a repository bean
public interface AdReportDataRepository extends
        JpaRepository<AdReportData, Long>,
        JpaSpecificationExecutor<AdReportData> {

    @Query(value = "SELECT * FROM ad_report_data " +
            "WHERE id IN (:ids) " +
            "ORDER BY id " +
            "LIMIT 20 OFFSET :offset", nativeQuery = true)
    List<AdReportData> findByIdsWithPagination(@Param("ids") List<String> ids,
                                               @Param("offset") int offset);


    // Optimized single query for ALL filter dimensions - with date filter
    @Query("SELECT DISTINCT a.mobileAppResolvedId, a.mobileAppName, a.domain, a.adUnitName, a.adUnitId, " +
            "a.inventoryFormatName, a.operatingSystemVersionName " +
            "FROM AdReportData a WHERE a.date BETWEEN :startDate AND :endDate " +
            "ORDER BY a.mobileAppName, a.inventoryFormatName, a.operatingSystemVersionName")
    List<Object[]> findAllDistinctFiltersByDateRange(@Param("startDate") LocalDate startDate, @Param("endDate") LocalDate endDate);


    // Method to check for existing records to prevent duplicates
    Optional<AdReportData> findByMobileAppResolvedIdAndDateAndAdUnitIdAndInventoryFormatNameAndOperatingSystemVersionName(
            String mobileAppResolvedId,
            LocalDate date,
            String adUnitId,
            String inventoryFormatName,
            String operatingSystemVersionName
    );

    // Method to find records for batch duplicate checking
    @Query("SELECT a FROM AdReportData a WHERE " +
            "a.mobileAppResolvedId = :mobileAppResolvedId AND " +
            "a.date = :date AND " +
            "a.adUnitId = :adUnitId AND " +
            "a.inventoryFormatName = :inventoryFormatName AND " +
            "a.operatingSystemVersionName = :operatingSystemVersionName")
    List<AdReportData> findDuplicateRecords(
            @Param("mobileAppResolvedId") String mobileAppResolvedId,
            @Param("date") LocalDate date,
            @Param("adUnitId") String adUnitId,
            @Param("inventoryFormatName") String inventoryFormatName,
            @Param("operatingSystemVersionName") String operatingSystemVersionName
    );

    // Single record UPSERT method for handling duplicates efficiently
    @Modifying
    @Transactional
    @Query(value = """
        INSERT INTO ad_report_data (
            mobile_app_resolved_id, mobile_app_name, domain, ad_unit_name, ad_unit_id,
            inventory_format_name, operating_system_version_name, operating_system_name,
            country_name, country_criteria_id, date,
            ad_exchange_total_requests, ad_exchange_responses_served, ad_exchange_match_rate,
            ad_exchange_line_item_level_impressions, ad_exchange_line_item_level_clicks,
            ad_exchange_line_item_level_ctr, average_ecpm, payout, ad_exchange_cost_per_click
        ) VALUES (
            :mobileAppResolvedId, :mobileAppName, :domain, :adUnitName, :adUnitId,
            :inventoryFormatName, :operatingSystemVersionName, :operatingSystemName,
            :countryName, :countryCriteriaId, :date,
            :adExchangeTotalRequests, :adExchangeResponsesServed, :adExchangeMatchRate,
            :adExchangeLineItemLevelImpressions, :adExchangeLineItemLevelClicks,
            :adExchangeLineItemLevelCtr, :averageEcpm, :payout, :adExchangeCostPerClick
        )
        ON CONFLICT (date, mobile_app_resolved_id, mobile_app_name, ad_unit_name, ad_unit_id, 
                    inventory_format_name, domain, operating_system_version_name, 
                    operating_system_name, country_name, country_criteria_id)
        DO UPDATE SET
            ad_exchange_total_requests = EXCLUDED.ad_exchange_total_requests,
            ad_exchange_responses_served = EXCLUDED.ad_exchange_responses_served,
            ad_exchange_match_rate = EXCLUDED.ad_exchange_match_rate,
            ad_exchange_line_item_level_impressions = EXCLUDED.ad_exchange_line_item_level_impressions,
            ad_exchange_line_item_level_clicks = EXCLUDED.ad_exchange_line_item_level_clicks,
            ad_exchange_line_item_level_ctr = EXCLUDED.ad_exchange_line_item_level_ctr,
            average_ecpm = EXCLUDED.average_ecpm,
            payout = EXCLUDED.payout,
            ad_exchange_cost_per_click = EXCLUDED.ad_exchange_cost_per_click
        """, nativeQuery = true)
    void upsertRecord(
            @Param("mobileAppResolvedId") String mobileAppResolvedId,
            @Param("mobileAppName") String mobileAppName,
            @Param("domain") String domain,
            @Param("adUnitName") String adUnitName,
            @Param("adUnitId") String adUnitId,
            @Param("inventoryFormatName") String inventoryFormatName,
            @Param("operatingSystemVersionName") String operatingSystemVersionName,
            @Param("operatingSystemName") String operatingSystemName,
            @Param("countryName") String countryName,
            @Param("countryCriteriaId") String countryCriteriaId,
            @Param("date") LocalDate date,
            @Param("adExchangeTotalRequests") Long adExchangeTotalRequests,
            @Param("adExchangeResponsesServed") Long adExchangeResponsesServed,
            @Param("adExchangeMatchRate") Double adExchangeMatchRate,
            @Param("adExchangeLineItemLevelImpressions") Long adExchangeLineItemLevelImpressions,
            @Param("adExchangeLineItemLevelClicks") Long adExchangeLineItemLevelClicks,
            @Param("adExchangeLineItemLevelCtr") Double adExchangeLineItemLevelCtr,
            @Param("averageEcpm") Double averageEcpm,
            @Param("payout") Double payout,
            @Param("adExchangeCostPerClick") Double adExchangeCostPerClick
    );

}