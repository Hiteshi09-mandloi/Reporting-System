package com.adtech.reportingsystem.repository; // Ensure this package matches your actual repository package

import com.adtech.reportingsystem.model.AdReportData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository; // Important: Ensure this is imported

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

}
