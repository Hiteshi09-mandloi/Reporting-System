package com.adtech.reportingsystem.service;

import com.adtech.reportingsystem.model.AdReportData;
import com.adtech.reportingsystem.repository.AdReportDataRepository;
import com.adtech.reportingsystem.dto.ReportRequest;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Tuple;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.*;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class ReportService {

    private static final Logger logger = LoggerFactory.getLogger(ReportService.class);

    @Autowired
    private AdReportDataRepository adReportDataRepository;

    @PersistenceContext
    private EntityManager entityManager;

    public List<String> getAvailableDimensions() {
        return Arrays.asList(
                "mobileAppResolvedId", "mobileAppName", "domain", "adUnitName", "adUnitId",
                "inventoryFormatName", "operatingSystemVersionName", "date"
        );
    }

    public List<String> getAvailableMetrics() {
        return Arrays.asList(
                "adExchangeTotalRequests", "adExchangeResponsesServed", "adExchangeMatchRate",
                "adExchangeLineItemLevelImpressions", "adExchangeLineItemLevelClicks",
                "adExchangeLineItemLevelCtr", "averageEcpm", "payout"
        );
    }

    public Map<String, List<String>> getAllFilters(String startDateStr, String endDateStr) {
        LocalDate startDate = LocalDate.parse(startDateStr);
        LocalDate endDate = LocalDate.parse(endDateStr);
        
        // Validate date range
        if (startDate.isAfter(endDate)) {
            throw new IllegalArgumentException("startDate cannot be after endDate");
        }
        
        logger.debug("Fetching all filters for date range: {} to {} using single optimized query", startDate, endDate);
        return extractAllFiltersFromSingleQuery(adReportDataRepository.findAllDistinctFiltersByDateRange(startDate, endDate));
    }

    private Map<String, List<String>> extractAllFiltersFromSingleQuery(List<Object[]> queryResults) {
        // Using LinkedHashSet to maintain order and ensure uniqueness
        Set<String> mobileAppResolvedIds = new LinkedHashSet<>();
        Set<String> mobileAppNames = new LinkedHashSet<>();
        Set<String> domains = new LinkedHashSet<>();
        Set<String> adUnitNames = new LinkedHashSet<>();
        Set<String> adUnitIds = new LinkedHashSet<>();
        Set<String> inventoryFormatNames = new LinkedHashSet<>();
        Set<String> operatingSystemVersionNames = new LinkedHashSet<>();
        
        // Extract all dimensions from single query result
        // Query returns: mobileAppResolvedId, mobileAppName, domain, adUnitName, adUnitId, inventoryFormatName, operatingSystemVersionName
        for (Object[] row : queryResults) {
            if (row[0] != null) mobileAppResolvedIds.add((String) row[0]);
            if (row[1] != null) mobileAppNames.add((String) row[1]);
            if (row[2] != null) domains.add((String) row[2]);
            if (row[3] != null) adUnitNames.add((String) row[3]);
            if (row[4] != null) adUnitIds.add((String) row[4]);
            if (row[5] != null) inventoryFormatNames.add((String) row[5]);
            if (row[6] != null) operatingSystemVersionNames.add((String) row[6]);
        }
        
        // Build comprehensive filter response
        Map<String, List<String>> filters = new HashMap<>();
        filters.put("mobileAppResolvedIds", new ArrayList<>(mobileAppResolvedIds));
        filters.put("mobileAppNames", new ArrayList<>(mobileAppNames));
        filters.put("domains", new ArrayList<>(domains));
        filters.put("adUnitNames", new ArrayList<>(adUnitNames));
        filters.put("adUnitIds", new ArrayList<>(adUnitIds));
        filters.put("inventoryFormatNames", new ArrayList<>(inventoryFormatNames));
        filters.put("operatingSystemVersionNames", new ArrayList<>(operatingSystemVersionNames));
        
        logger.debug("Extracted from single query: {} app IDs, {} app names, {} domains, {} ad unit names, {} ad unit IDs, {} inventory formats, {} OS versions", 
                    mobileAppResolvedIds.size(), mobileAppNames.size(), domains.size(), 
                    adUnitNames.size(), adUnitIds.size(), inventoryFormatNames.size(), operatingSystemVersionNames.size());
        
        return filters;
    }

    public List<Map<String, Object>> getReport(ReportRequest request) {
        logger.debug("Fetching report data with request: {}", request);

        // If no dimensions and metrics specified, return all table data
        if ((request.getGroupByDimensions() == null || request.getGroupByDimensions().isEmpty()) && 
            (request.getMetrics() == null || request.getMetrics().isEmpty())) {
            return getAllTableData(request);
        }

        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> cq = cb.createTupleQuery();
        Root<AdReportData> root = cq.from(AdReportData.class);

        List<Predicate> predicates = buildReportPredicates(request, cb, root);
        if (!predicates.isEmpty()) {
            cq.where(cb.and(predicates.toArray(new Predicate[0])));
        }

        List<Selection<?>> selections = new ArrayList<>();

        // Add dimensions (actual column values, not aggregated)
        if (request.getGroupByDimensions() != null && !request.getGroupByDimensions().isEmpty()) {
            for (String dim : request.getGroupByDimensions()) {
                String actualFieldName = mapToActualFieldName(dim);
                selections.add(root.get(actualFieldName).alias(dim));
            }
        }

        // Add metrics (actual column values, not aggregated)
        if (request.getMetrics() != null && !request.getMetrics().isEmpty()) {
            for (String metric : request.getMetrics()) {
                String actualFieldName = mapToActualFieldName(metric);
                selections.add(root.get(actualFieldName).alias(metric));
            }
        }

        cq.multiselect(selections.toArray(new Selection[0]));

        TypedQuery<Tuple> typedQuery = entityManager.createQuery(cq);
        
        if (request.getOffset() != null && request.getOffset() > 0) {
            typedQuery.setFirstResult(request.getOffset());
        }
        if (request.getLimit() != null && request.getLimit() > 0) {
            typedQuery.setMaxResults(request.getLimit());
        }
        
        List<Tuple> results = typedQuery.getResultList();

        List<Map<String, Object>> mappedResults = new ArrayList<>();
        for (Tuple tuple : results) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (Selection<?> selection : selections) {
                String alias = selection.getAlias();
                Object value = tuple.get(alias);
                if (value instanceof LocalDate) {
                    row.put(alias, ((LocalDate) value).format(DateTimeFormatter.ISO_LOCAL_DATE));
                } else {
                    row.put(alias, value);
                }
            }
            mappedResults.add(row);
        }

        return mappedResults;
    }

    private String mapToActualFieldName(String inputName) {
        switch (inputName) {
            case "mobile_app_resolved_id":
                return "mobileAppResolvedId";
            case "mobile_app_name":
                return "mobileAppName";
            case "domain":
                return "domain";
            case "ad_unit_name":
                return "adUnitName";
            case "ad_unit_id":
                return "adUnitId";
            case "inventory_format_name":
                return "inventoryFormatName";
            case "operating_system_version_name":
                return "operatingSystemVersionName";
            case "date":
                return "date";
            case "ad_exchange_total_requests":
                return "adExchangeTotalRequests";
            case "ad_exchange_responses_served":
                return "adExchangeResponsesServed";
            case "ad_exchange_match_rate":
                return "adExchangeMatchRate";
            case "ad_exchange_line_item_level_impressions":
                return "adExchangeLineItemLevelImpressions";
            case "ad_exchange_line_item_level_clicks":
                return "adExchangeLineItemLevelClicks";
            case "ad_exchange_line_item_level_ctr":
                return "adExchangeLineItemLevelCtr";
            case "average_ecpm":
                return "averageEcpm";
            case "payout":
                return "payout";
            default:
                return inputName;
        }
    }

    private List<Map<String, Object>>  getAllTableData(ReportRequest request) {
        logger.debug("Fetching all table data with filters");

        Specification<AdReportData> spec = (root, query, criteriaBuilder) -> {
            List<Predicate> allPredicates = buildReportPredicates(request, criteriaBuilder, root);
            return allPredicates.isEmpty() ? null : criteriaBuilder.and(allPredicates.toArray(new Predicate[0]));
        };

        List<AdReportData> data;
        if (request.getOffset() != null && request.getLimit() != null) {
            Pageable pageable = PageRequest.of(request.getOffset() / request.getLimit(), request.getLimit());
            data = adReportDataRepository.findAll(spec, pageable).getContent();
        } else {
            data = adReportDataRepository.findAll(spec);
        }

        List<Map<String, Object>> result = new ArrayList<>();
        for (AdReportData item : data) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("mobile_app_resolved_id", item.getMobileAppResolvedId());
            row.put("mobile_app_name", item.getMobileAppName());
            row.put("domain", item.getDomain());
            row.put("ad_unit_name", item.getAdUnitName());
            row.put("ad_unit_id", item.getAdUnitId());
            row.put("inventory_format_name", item.getInventoryFormatName());
            row.put("operating_system_version_name", item.getOperatingSystemVersionName());
            row.put("date", item.getDate() != null ? item.getDate().format(DateTimeFormatter.ISO_LOCAL_DATE) : null);
            row.put("ad_exchange_total_requests", item.getAdExchangeTotalRequests());
            row.put("ad_exchange_responses_served", item.getAdExchangeResponsesServed());
            row.put("ad_exchange_match_rate", item.getAdExchangeMatchRate());
            row.put("ad_exchange_line_item_level_impressions", item.getAdExchangeLineItemLevelImpressions());
            row.put("ad_exchange_line_item_level_clicks", item.getAdExchangeLineItemLevelClicks());
            row.put("ad_exchange_line_item_level_ctr", item.getAdExchangeLineItemLevelCtr());
            row.put("average_ecpm", item.getAverageEcpm());
            row.put("payout", item.getPayout());
            result.add(row);
        }

        return result;
    }


    public Map<String, Object> getAggregateReport(ReportRequest request) {
        logger.debug("Fetching aggregate report data with request: {}", request);

        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> cq = cb.createTupleQuery();
        Root<AdReportData> root = cq.from(AdReportData.class);

        List<Predicate> predicates = buildReportPredicates(request, cb, root);
        if (!predicates.isEmpty()) {
            cq.where(cb.and(predicates.toArray(new Predicate[0])));
        }

        List<Selection<?>> selections = new ArrayList<>();
        
        selections.add(cb.sum(root.get("adExchangeTotalRequests")).alias("total_requests"));
        selections.add(cb.sum(root.get("adExchangeLineItemLevelImpressions")).alias("total_impressions"));
        selections.add(cb.sum(root.get("adExchangeLineItemLevelClicks")).alias("total_clicks"));
        selections.add(cb.sum(root.get("payout")).alias("total_payout"));

        cq.multiselect(selections.toArray(new Selection[0]));

        TypedQuery<Tuple> typedQuery = entityManager.createQuery(cq);
        Tuple result = typedQuery.getSingleResult();

        Map<String, Object> aggregateData = new LinkedHashMap<>();
        aggregateData.put("total_requests", result.get("total_requests"));
        aggregateData.put("total_impressions", result.get("total_impressions"));
        aggregateData.put("total_clicks", result.get("total_clicks"));
        aggregateData.put("total_payout", result.get("total_payout"));

        return aggregateData;
    }

    private List<Predicate> buildReportPredicates(ReportRequest request, CriteriaBuilder cb, Root<AdReportData> root) {
        List<Predicate> predicates = new ArrayList<>();

        if (request.getStartDate() != null && request.getEndDate() != null) {
            LocalDate startDate = LocalDate.parse(request.getStartDate());
            LocalDate endDate = LocalDate.parse(request.getEndDate());
            predicates.add(cb.between(root.get("date"), startDate, endDate));
        }

        if (request.getMobileAppNames() != null && !request.getMobileAppNames().isEmpty()) {
            predicates.add(root.get("mobileAppName").in(request.getMobileAppNames()));
        }
        if (request.getInventoryFormatNames() != null && !request.getInventoryFormatNames().isEmpty()) {
            predicates.add(root.get("inventoryFormatName").in(request.getInventoryFormatNames()));
        }
        if (request.getOperatingSystemVersionNames() != null && !request.getOperatingSystemVersionNames().isEmpty()) {
            predicates.add(root.get("operatingSystemVersionName").in(request.getOperatingSystemVersionNames()));
        }
        if (request.getMobileAppResolvedIds() != null && !request.getMobileAppResolvedIds().isEmpty()) {
            predicates.add(root.get("mobileAppResolvedId").in(request.getMobileAppResolvedIds()));
        }
        if (request.getDomains() != null && !request.getDomains().isEmpty()) {
            predicates.add(root.get("domain").in(request.getDomains()));
        }
        if (request.getAdUnitNames() != null && !request.getAdUnitNames().isEmpty()) {
            predicates.add(root.get("adUnitName").in(request.getAdUnitNames()));
        }
        if (request.getAdUnitIds() != null && !request.getAdUnitIds().isEmpty()) {
            predicates.add(root.get("adUnitId").in(request.getAdUnitIds()));
        }

        return predicates;
    }
}
