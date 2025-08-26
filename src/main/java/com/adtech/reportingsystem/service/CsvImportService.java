package com.adtech.reportingsystem.service;

import com.adtech.reportingsystem.model.AdReportData;
import com.adtech.reportingsystem.repository.AdReportDataRepository;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class CsvImportService {

    public static class ImportProgress {
        private volatile long totalRecords = 0;
        private volatile long processedRecords = 0;
        private volatile long savedRecords = 0;
        private volatile long errorRecords = 0;
        private volatile String currentPhase = "Starting";
        private volatile int progressPercentage = 0;

        public long getTotalRecords() { return totalRecords; }
        public void setTotalRecords(long totalRecords) { this.totalRecords = totalRecords; }
        public long getProcessedRecords() { return processedRecords; }
        public void setProcessedRecords(long processedRecords) { this.processedRecords = processedRecords; }
        public long getSavedRecords() { return savedRecords; }
        public void setSavedRecords(long savedRecords) { this.savedRecords = savedRecords; }
        public long getErrorRecords() { return errorRecords; }
        public void setErrorRecords(long errorRecords) { this.errorRecords = errorRecords; }
        public String getCurrentPhase() { return currentPhase; }
        public void setCurrentPhase(String currentPhase) { this.currentPhase = currentPhase; }
        public int getProgressPercentage() { return progressPercentage; }
        public void setProgressPercentage(int progressPercentage) { this.progressPercentage = progressPercentage; }

        public void updateProgress() {
            if (totalRecords > 0) {
                // Calculate progress based on current phase with 10% increments
                switch (currentPhase) {
                    case "File uploaded, starting processing..." -> {
                        this.progressPercentage = 10;
                    }
                    case "Reading and validating file headers..." -> {
                        this.progressPercentage = 20;
                    }
                    case "Counting total records..." -> {
                        this.progressPercentage = 30;
                    }
                    case "Processing records..." -> {
                        // Processing phase: 40-80% based on processed records
                        int baseProgress = 40;
                        int processingProgress = (int) ((processedRecords * 40) / totalRecords);
                        this.progressPercentage = baseProgress + processingProgress;
                        // Round to nearest 10%
                        this.progressPercentage = ((this.progressPercentage + 5) / 10) * 10;
                        // Ensure it stays within 40-80% range
                        this.progressPercentage = Math.max(40, Math.min(80, this.progressPercentage));
                    }
                    case "Saving to database..." -> {
                        // Saving phase: 90%
                        this.progressPercentage = 90;
                    }
                    case "Completed" -> {
                        // Only 100% when actually completed
                        this.progressPercentage = 100;
                    }
                    default -> {
                        // Other phases: keep current percentage or set to 10%
                        if (this.progressPercentage == 0) {
                            this.progressPercentage = 10;
                        }
                    }
                }
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(CsvImportService.class);

    @Autowired
    private AdReportDataRepository adReportDataRepository;

    private final ExecutorService executorService = Executors.newFixedThreadPool(5); // For main job submission
    private final ExecutorService chunkSaveExecutor = Executors.newFixedThreadPool(8); // For parallel chunk saves

    private final AtomicLong importJobCounter = new AtomicLong();
    private final ConcurrentHashMap<Long, String> importStatusMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ImportProgress> importProgressMap = new ConcurrentHashMap<>();

    private static final Map<String, String> HEADER_MAPPING = new HashMap<>();

    static {
        HEADER_MAPPING.put("App ID", "mobile_app_resolved_id");
        HEADER_MAPPING.put("App Name", "mobile_app_name");
        HEADER_MAPPING.put("Domain", "domain");
        HEADER_MAPPING.put("Ad Unit", "ad_unit_name");
        HEADER_MAPPING.put("Ad Unit ID", "ad_unit_id");
        HEADER_MAPPING.put("Inventory Format", "inventory_format_name");
        HEADER_MAPPING.put("OS Version", "operating_system_version_name");
        HEADER_MAPPING.put("OS Name", "operating_system_name");
        HEADER_MAPPING.put("OS", "operating_system_name"); // Additional mapping
        HEADER_MAPPING.put("Country Name", "country_name");
        HEADER_MAPPING.put("Country", "country_name"); // Additional mapping
        HEADER_MAPPING.put("Country Criteria ID", "country_criteria_id");
        HEADER_MAPPING.put("Country ID", "country_criteria_id"); // Additional mapping
        HEADER_MAPPING.put("Date", "date");
        HEADER_MAPPING.put("Total Requests", "ad_exchange_total_requests");
        HEADER_MAPPING.put("Responses Served", "ad_exchange_responses_served");
        HEADER_MAPPING.put("Match Rate", "ad_exchange_match_rate");
        HEADER_MAPPING.put("Impressions", "ad_exchange_line_item_level_impressions");
        HEADER_MAPPING.put("Clicks", "ad_exchange_line_item_level_clicks");
        HEADER_MAPPING.put("CTR", "ad_exchange_line_item_level_ctr");
        HEADER_MAPPING.put("Average eCPM", "average_ecpm");
        HEADER_MAPPING.put("Payout", "payout");
        HEADER_MAPPING.put("Cost Per Click", "ad_exchange_cost_per_click");
    }

    public Long importCsvData(MultipartFile file) throws IOException {
        long jobId = importJobCounter.incrementAndGet();
        importStatusMap.put(jobId, "IN_PROGRESS");

        // Initialize progress tracking
        ImportProgress progress = new ImportProgress();
        progress.setCurrentPhase("File uploaded, processing started...");
        importProgressMap.put(jobId, progress);

        logger.info("Import job {} initiated. Status: IN_PROGRESS.", jobId);

        // IMPORTANT: Move ALL file processing to background thread for immediate response
        String originalFilename = file.getOriginalFilename();

        // Submit to background thread immediately - no file reading in main thread
        executorService.submit(() -> {
            ImportProgress bgProgress = importProgressMap.get(jobId);
            try {
                // Update progress - now reading file in background
                bgProgress.setCurrentPhase("Reading uploaded file...");
                bgProgress.updateProgress();

                // Read file bytes in background thread to avoid blocking upload API
                byte[] fileBytes = file.getBytes();

                // Now start actual CSV processing
                processCsvFileAsync(fileBytes, originalFilename, jobId);
            } catch (IOException e) {
                String errorMessage = "FAILED: Error reading uploaded file - " + e.getMessage();
                logger.error("Import job {}: {}", jobId, errorMessage, e);
                failJob(jobId, errorMessage, originalFilename);
            } catch (Exception e) {
                String errorMessage = "FAILED: Unexpected error during file processing - " + e.getMessage();
                logger.error("Import job {}: {}", jobId, errorMessage, e);
                failJob(jobId, errorMessage, originalFilename);
            }
        });

        return jobId; // Return immediately - no waiting for file processing
    }

    @Async("csvImportTaskExecutor")
    @Transactional
    protected void processCsvFileAsync(byte[] fileBytes, String originalFilename, long jobId) {
        ImportProgress progress = importProgressMap.get(jobId);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new java.io.ByteArrayInputStream(fileBytes))); CSVReader csvReader = new CSVReader(reader)) {

            progress.setCurrentPhase("Reading and validating file headers...");

            // Header mapping
            String[] header = csvReader.readNext();
            if (header == null || header.length == 0) {
                failJob(jobId, "FAILED: Empty file or no header.", originalFilename);
                return;
            }
            String[] mappedHeader = mapHeaders(header);
            if (!validateHeaders(mappedHeader, jobId, originalFilename)) {
                return;
            }

            progress.setCurrentPhase("Counting total records...");

            // First pass: count total records
            long totalRecords = 0;
            while (csvReader.readNext() != null) {
                totalRecords++;
            }
            progress.setTotalRecords(totalRecords);
            progress.setCurrentPhase("Processing records...");

            // Reset reader for second pass
            try (BufferedReader reader2 = new BufferedReader(new InputStreamReader(new java.io.ByteArrayInputStream(fileBytes)));
                 CSVReader csvReader2 = new CSVReader(reader2)) {

                csvReader2.readNext(); // Skip header

                // Parallel chunk saving setup
                final int BATCH_SIZE = 5000;
                List<Future<?>> saveTasks = new ArrayList<>();
                List<AdReportData> batch = new ArrayList<>(BATCH_SIZE);
                long processedRecords = 0;
                long errorRecords = 0;

                String[] line;
                long lineNumber = 1; // Start from 1 (header is line 0)
                while ((line = csvReader2.readNext()) != null) {
                    lineNumber++;
                    try {
                        AdReportData data = parseCsvLine(line, mappedHeader);

                        // Validate that all required key fields are present
                        validateRequiredFields(data);

                        batch.add(data);
                        if (batch.size() >= BATCH_SIZE) {
                            List<AdReportData> chunkToSave = new ArrayList<>(batch);
                            batch.clear();
                            saveTasks.add(saveChunkAsync(chunkToSave, jobId));
                        }
                        processedRecords++;

                        // Update progress every 1000 records
                        if (processedRecords % 1000 == 0) {
                            progress.setProcessedRecords(processedRecords);
                            progress.updateProgress();
                        }

                    } catch (Exception e) {
                        errorRecords++;
                        String errorMessage = String.format(
                                "Skipping invalid row on line %d. Data: [%s]. Error: %s",
                                lineNumber,
                                String.join(",", line),
                                e.getMessage()
                        );
                        logger.warn("Import job {}: {}", jobId, errorMessage);

                        // Update progress to include error count
                        progress.setErrorRecords(errorRecords);

                        // Continue processing instead of failing
                    }
                }

                if (!batch.isEmpty()) {
                    List<AdReportData> chunkToSave = new ArrayList<>(batch);
                    saveTasks.add(saveChunkAsync(chunkToSave, jobId));
                }

                progress.setCurrentPhase("Saving to database...");
                progress.setProcessedRecords(processedRecords);
                progress.updateProgress();

                // Wait for all save tasks to finish - fail immediately if any task fails
                int completedTasks = 0;
                for (Future<?> task : saveTasks) {
                    try {
                        task.get(); // Wait synchronously — will throw exception if save failed
                        completedTasks++;
                        // Update progress during saving phase (90% base)
                        if (!saveTasks.isEmpty()) {
                            // Only update if we have significant progress (every 25% of tasks completed)
                            if (completedTasks % Math.max(1, saveTasks.size() / 4) == 0 || completedTasks == saveTasks.size()) {
                                progress.setProgressPercentage(90);
                            }
                        }
                    } catch (Exception e) {
                        String errorMessage = String.format(
                                "FAILED: Database save error in async task. Error: %s",
                                e.getCause() != null ? e.getCause().getMessage() : e.getMessage()
                        );
                        logger.error("Import job {}: {}", jobId, errorMessage, e);
                        failJob(jobId, errorMessage, originalFilename);
                        return; // Stop processing immediately
                    }
                }

                progress.setCurrentPhase("Completed");
                progress.setSavedRecords(processedRecords); // All processed records are saved
                progress.updateProgress();

                long validRecords = processedRecords;
                importStatusMap.put(jobId, String.format("COMPLETED: Processed %d valid records, skipped %d invalid rows.",
                        validRecords, errorRecords));
                logger.info("Import job {} COMPLETED. Processed {} valid records, skipped {} invalid rows.",
                        jobId, validRecords, errorRecords);

            }

        } catch (CsvValidationException e) {
            failJob(jobId, "FAILED: CSV format validation error - " + e.getMessage(), originalFilename);
        } catch (IOException e) {
            failJob(jobId, "FAILED: I/O error during file processing - " + e.getMessage(), originalFilename);
        } catch (Exception e) {
            failJob(jobId, "FAILED: An unexpected error occurred - " + e.getMessage(), originalFilename);
        }
    }

    private Future<?> saveChunkAsync(List<AdReportData> chunk, long jobId) {
        return chunkSaveExecutor.submit(() -> {
            try {
                // Use UPSERT for each record to handle duplicates by updating them
                for (AdReportData data : chunk) {
                    adReportDataRepository.upsertRecord(
                            data.getMobileAppResolvedId(),
                            data.getMobileAppName(),
                            data.getDomain(),
                            data.getAdUnitName(),
                            data.getAdUnitId(),
                            data.getInventoryFormatName(),
                            data.getOperatingSystemVersionName(),
                            data.getOperatingSystemName(),
                            data.getCountryName(),
                            data.getCountryCriteriaId(),
                            data.getDate(),
                            data.getAdExchangeTotalRequests(),
                            data.getAdExchangeResponsesServed(),
                            data.getAdExchangeMatchRate(),
                            data.getAdExchangeLineItemLevelImpressions(),
                            data.getAdExchangeLineItemLevelClicks(),
                            data.getAdExchangeLineItemLevelCtr(),
                            data.getAverageEcpm(),
                            data.getPayout(),
                            data.getAdExchangeCostPerClick()
                    );
                }
                logger.debug("Import job {}: UPSERTED a chunk of {} records.", jobId, chunk.size());
            } catch (Exception e) {
                String errorDetails = String.format(
                        "Database error while upserting chunk of %d records. Error: %s",
                        chunk.size(),
                        e.getMessage()
                );
                logger.error("Import job {}: {}", jobId, errorDetails, e);
                throw new RuntimeException(errorDetails, e);
            }
        });
    }



    private void failJob(long jobId, String message, String filename) {
        importStatusMap.put(jobId, message);
        logger.error("Import job {} FAILED: {} Filename: {}", jobId, message, filename);
    }

    public String getImportStatus(Long jobId) {
        return importStatusMap.getOrDefault(jobId, "NOT_FOUND");
    }

    public ImportProgress getImportProgress(Long jobId) {
        return importProgressMap.getOrDefault(jobId, null);
    }

    private static final DateTimeFormatter[] DATE_FORMATS = {
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),
            DateTimeFormatter.ISO_LOCAL_DATE
    };

    private String[] mapHeaders(String[] header) {
        String[] mappedHeader = new String[header.length];
        for (int i = 0; i < header.length; i++) {
            String trimmedHeader = header[i].trim();
            mappedHeader[i] = HEADER_MAPPING.getOrDefault(trimmedHeader, trimmedHeader);
        }
        return mappedHeader;
    }

    private boolean validateHeaders(String[] mappedHeader, long jobId, String filename) {
        // All 11 dimensions are mandatory for proper duplicate detection
        List<String> requiredDimensions = List.of(
                "date", "mobile_app_resolved_id", "mobile_app_name", "ad_unit_name", "ad_unit_id",
                "inventory_format_name", "domain", "operating_system_version_name",
                "operating_system_name", "country_name", "country_criteria_id"
        );

        List<String> headerList = List.of(mappedHeader);
        List<String> missingHeaders = new ArrayList<>();

        for (String required : requiredDimensions) {
            if (!headerList.contains(required)) {
                missingHeaders.add(required);
            }
        }

        if (!missingHeaders.isEmpty()) {
            failJob(jobId, "FAILED: Missing required dimension columns in CSV header: " + missingHeaders +
                    ". All 11 dimensions are mandatory.", filename);
            return false;
        }
        return true;
    }

    private void validateRequiredFields(AdReportData data) {
        List<String> missingFields = new ArrayList<>();

        // Validate ALL 11 dimension fields - all are mandatory
        if (data.getMobileAppResolvedId() == null || data.getMobileAppResolvedId().trim().isEmpty()) {
            missingFields.add("mobile_app_resolved_id (App ID)");
        }
        if (data.getMobileAppName() == null || data.getMobileAppName().trim().isEmpty()) {
            missingFields.add("mobile_app_name (App Name)");
        }
        if (data.getDomain() == null || data.getDomain().trim().isEmpty()) {
            missingFields.add("domain (Domain)");
        }
        if (data.getAdUnitName() == null || data.getAdUnitName().trim().isEmpty()) {
            missingFields.add("ad_unit_name (Ad Unit)");
        }
        if (data.getAdUnitId() == null || data.getAdUnitId().trim().isEmpty()) {
            missingFields.add("ad_unit_id (Ad Unit ID)");
        }
        if (data.getInventoryFormatName() == null || data.getInventoryFormatName().trim().isEmpty()) {
            missingFields.add("inventory_format_name (Inventory Format)");
        }
        if (data.getOperatingSystemVersionName() == null || data.getOperatingSystemVersionName().trim().isEmpty()) {
            missingFields.add("operating_system_version_name (OS Version)");
        }
        if (data.getOperatingSystemName() == null || data.getOperatingSystemName().trim().isEmpty()) {
            missingFields.add("operating_system_name (OS Name)");
        }
        if (data.getCountryName() == null || data.getCountryName().trim().isEmpty()) {
            missingFields.add("country_name (Country Name)");
        }
        if (data.getCountryCriteriaId() == null || data.getCountryCriteriaId().trim().isEmpty()) {
            missingFields.add("country_criteria_id (Country Criteria ID)");
        }
        if (data.getDate() == null) {
            missingFields.add("date");
        }

        if (!missingFields.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Missing required dimension fields: %s. All 11 dimensions are mandatory for proper duplicate detection.",
                            String.join(", ", missingFields))
            );
        }
    }

    private LocalDate parseFlexibleDate(String value) {
        for (DateTimeFormatter formatter : DATE_FORMATS) {
            try {
                return LocalDate.parse(value, formatter);
            } catch (DateTimeParseException ignored) {
            }
        }
        throw new IllegalArgumentException("Unsupported date format: " + value);
    }

    private AdReportData parseCsvLine(String[] line, String[] mappedHeader) {
        AdReportData data = new AdReportData();
        for (int i = 0; i < mappedHeader.length; i++) {
            String colName = mappedHeader[i].trim();
            String value = line[i].trim();

            if (value.isEmpty() || value.isBlank() || value.equalsIgnoreCase("null") || value.equalsIgnoreCase("N/A")) {
                continue;
            }

            try {
                switch (colName) {
                    case "mobile_app_resolved_id" ->
                            data.setMobileAppResolvedId(value);
                    case "mobile_app_name" ->
                            data.setMobileAppName(value);
                    case "domain" ->
                            data.setDomain(value);
                    case "ad_unit_name" ->
                            data.setAdUnitName(value);
                    case "ad_unit_id" ->
                            data.setAdUnitId(value);
                    case "inventory_format_name" ->
                            data.setInventoryFormatName(value);
                    case "operating_system_version_name" ->
                            data.setOperatingSystemVersionName(value);
                    case "operating_system_name" ->
                            data.setOperatingSystemName(value);
                    case "country_name" ->
                            data.setCountryName(value);
                    case "country_criteria_id" ->
                            data.setCountryCriteriaId(value);
                    case "date" ->
                            data.setDate(parseFlexibleDate(value));
                    case "ad_exchange_total_requests" ->
                            data.setAdExchangeTotalRequests(Long.valueOf(value));
                    case "ad_exchange_responses_served" ->
                            data.setAdExchangeResponsesServed(Long.valueOf(value));
                    case "ad_exchange_match_rate" ->
                            data.setAdExchangeMatchRate(Double.valueOf(value));
                    case "ad_exchange_line_item_level_impressions" ->
                            data.setAdExchangeLineItemLevelImpressions(Long.valueOf(value));
                    case "ad_exchange_line_item_level_clicks" ->
                            data.setAdExchangeLineItemLevelClicks(Long.valueOf(value));
                    case "ad_exchange_line_item_level_ctr" ->
                            data.setAdExchangeLineItemLevelCtr(Double.valueOf(value));
                    case "average_ecpm" ->
                            data.setAverageEcpm(Double.valueOf(value));
                    case "payout" ->
                            data.setPayout(Double.valueOf(value));
                    case "ad_exchange_cost_per_click" ->
                            data.setAdExchangeCostPerClick(Double.valueOf(value));
                    default ->
                            logger.warn("Unexpected column '{}' with value '{}'.", colName, value);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid number format for column '" + colName + "' with value '" + value + "'. Expected a valid number.", e);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid date format for column '" + colName + "' with value '" + value + "'. Expected format: YYYY-MM-DD, MM/dd/yyyy, or dd/MM/yyyy.", e);
            } catch (Exception e) {
                throw new IllegalArgumentException("Error processing column '" + colName + "' with value '" + value + "': " + e.getMessage(), e);
            }
        }
        return data;
    }

    @PreDestroy
    public void shutdownExecutor() {
        logger.info("Shutting down executor services.");
        executorService.shutdown();
        chunkSaveExecutor.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
            if (!chunkSaveExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                chunkSaveExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            chunkSaveExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
