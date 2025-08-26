package com.adtech.reportingsystem.controller;

import com.adtech.reportingsystem.service.ReportService;
import com.adtech.reportingsystem.service.CsvImportService;
import com.adtech.reportingsystem.dto.ReportRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/reports")
public class AdReportController {

    private static final Logger logger = LoggerFactory.getLogger(AdReportController.class);

    @Autowired
    private ReportService reportService;

    @Autowired
    private CsvImportService csvImportService;


    // New endpoint for getting report data with flexible payload
    @PostMapping("/getreport")
    public ResponseEntity<List<Map<String, Object>>> getReport(@RequestBody ReportRequest request) {
        logger.info("Received report request: {}", request);
        List<Map<String, Object>> reportData = reportService.getReport(request);
        return ResponseEntity.ok(reportData);
    }

    // Updated aggregate endpoint for getting totals only
    @PostMapping("/aggregate")
    public ResponseEntity<Map<String, Object>> getAggregateReport(@RequestBody ReportRequest request) {
        logger.info("Received aggregate report request: {}", request);
        Map<String, Object> aggregateData = reportService.getAggregateReport(request);
        return ResponseEntity.ok(aggregateData);
    }

    // An endpoint to upload CSV files
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<String> uploadCsvFile(@RequestParam("file") MultipartFile file) {
        logger.info("Received file upload request for: {}", file.getOriginalFilename());
        if (file.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please select a file to upload.");
        }

        try {
            Long jobId = csvImportService.importCsvData(file);
            // Return the actual jobId to the client
            return ResponseEntity.ok("CSV import started successfully with job ID: " + jobId + " for: " + file.getOriginalFilename());
        } catch (Exception e) {
            logger.error("Error processing CSV file upload: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to process CSV import: " + e.getMessage());
        }
    }


    // Unified endpoint to get all filter options with mandatory date filtering
    @GetMapping("/filters")
    public ResponseEntity<?> getAllFilters(
            @RequestParam(required = true) String startDate,
            @RequestParam(required = true) String endDate) {

        // Validate that dates are provided and not empty
        if (startDate == null || startDate.trim().isEmpty() || endDate == null || endDate.trim().isEmpty()) {
            logger.warn("Missing required date parameters: startDate={}, endDate={}", startDate, endDate);
            return ResponseEntity.badRequest()
                    .body("Both startDate and endDate are required parameters. Format: YYYY-MM-DD");
        }

        try {
            logger.info("Fetching all filter options with date range: {} to {}", startDate, endDate);
            Map<String, List<String>> filters = reportService.getAllFilters(startDate, endDate);
            return ResponseEntity.ok(filters);
        } catch (Exception e) {
            logger.error("Error fetching filters for date range {} to {}: {}", startDate, endDate, e.getMessage());
            return ResponseEntity.badRequest()
                    .body("Invalid date format. Please use YYYY-MM-DD format. Error: " + e.getMessage());
        }
    }

    // Endpoint to get import progress
    @GetMapping("/upload/progress/{jobId}")
    public ResponseEntity<?> getImportProgress(@PathVariable Long jobId) {
        try {
            CsvImportService.ImportProgress progress = csvImportService.getImportProgress(jobId);
            if (progress == null) {
                return ResponseEntity.notFound().build();
            }

            // Check if job has failed - return error status for client to handle
            String jobStatus = csvImportService.getImportStatus(jobId);
            if (jobStatus != null && jobStatus.startsWith("FAILED:")) {
                // Return HTTP 500 with error details so client can show error and stop polling
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(Map.of(
                                "error", true,
                                "message", jobStatus,
                                "progress", progress
                        ));
            }

            return ResponseEntity.ok(progress);
        } catch (Exception e) {
            logger.error("Error fetching import progress for job {}: {}", jobId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to get import progress: " + e.getMessage());
        }
    }
}