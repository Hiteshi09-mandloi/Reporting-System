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
    //importJobCounter → generates unique job IDs.
    private final ConcurrentHashMap<Long, String> importStatusMap = new ConcurrentHashMap<>();
    //importStatusMap → stores status of each job (IN_PROGRESS, COMPLETED, FAILED).
    private final ConcurrentHashMap<Long, AtomicLong> duplicateCountMap = new ConcurrentHashMap<>();
    //To keep track of how many duplicates have been found for each ID/job.
    //Example:
    //Job ID = 101, found 5 duplicates → stored as duplicateCountMap.put(101L, new AtomicLong(5));
    private final ConcurrentHashMap<Long, ImportProgress> importProgressMap = new ConcurrentHashMap<>();
    //To keep track of the progress of each CSV import job.
    //For example:
    //
    //Job ID 101 → 40% completed
    //
    //Job ID 102 → 75% complete

    private static final Map<String, String> HEADER_MAPPING = new HashMap<>();

    static {
        HEADER_MAPPING.put("App ID", "mobile_app_resolved_id");
        HEADER_MAPPING.put("App Name", "mobile_app_name");
        HEADER_MAPPING.put("Domain", "domain");
        HEADER_MAPPING.put("Ad Unit", "ad_unit_name");
        HEADER_MAPPING.put("Ad Unit ID", "ad_unit_id");
        HEADER_MAPPING.put("Inventory Format", "inventory_format_name");
        HEADER_MAPPING.put("OS Version", "operating_system_version_name");
        HEADER_MAPPING.put("Date", "date");
        HEADER_MAPPING.put("Total Requests", "ad_exchange_total_requests");
        HEADER_MAPPING.put("Responses Served", "ad_exchange_responses_served");
        HEADER_MAPPING.put("Match Rate", "ad_exchange_match_rate");
        HEADER_MAPPING.put("Impressions", "ad_exchange_line_item_level_impressions");
        HEADER_MAPPING.put("Clicks", "ad_exchange_line_item_level_clicks");
        HEADER_MAPPING.put("CTR", "ad_exchange_line_item_level_ctr");
        HEADER_MAPPING.put("Average eCPM", "average_ecpm");
        HEADER_MAPPING.put("Payout", "payout");
    }

    public Long importCsvData(MultipartFile file) throws IOException {
        long jobId = importJobCounter.incrementAndGet();
        importStatusMap.put(jobId, "IN_PROGRESS");
        duplicateCountMap.put(jobId, new AtomicLong(0));
        
        // Initialize progress tracking
        ImportProgress progress = new ImportProgress();
        progress.setCurrentPhase("File uploaded, starting processing...");
        importProgressMap.put(jobId, progress);
        
        logger.info("Import job {} initiated. Status: IN_PROGRESS.", jobId);

        byte[] fileBytes = file.getBytes();
        String originalFilename = file.getOriginalFilename();

        executorService.submit(() -> processCsvFileAsync(fileBytes, originalFilename, jobId));
        return jobId;
    }
    /*
    * Start async job.

Validate headers.

Count total rows.

Re-read and parse file.

Batch records (5000).

Save batches asynchronously.

Track progress + errors.

Wait for all save tasks.

Mark job completed.

Handle failures gracefully.
* This method allows you to upload huge CSVs,
*  process them efficiently in batches,
* track progress per job, and update users in real-time while handling failures cleanly.

   */
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
                while ((line = csvReader2.readNext()) != null) {
                    try {
                        AdReportData data = parseCsvLine(line, mappedHeader);
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
                        logger.error("Import job {}: Error parsing CSV line: {} - {}", jobId, String.join(",", line), e.getMessage(), e);
                        errorRecords++;
                        progress.setErrorRecords(errorRecords);
                    }
                }

                if (!batch.isEmpty()) {
                    List<AdReportData> chunkToSave = new ArrayList<>(batch);
                    saveTasks.add(saveChunkAsync(chunkToSave, jobId));
                }

                progress.setCurrentPhase("Saving to database...");
                progress.setProcessedRecords(processedRecords);
                progress.updateProgress();

                // Wait for all save tasks to finish
                int completedTasks = 0;
                for (Future<?> task : saveTasks) {
                    task.get(); // Wait synchronously — avoids marking job complete before all saves are done
                    completedTasks++;
                    // Update progress during saving phase (90% base)
                    if (!saveTasks.isEmpty()) {
                        // Only update if we have significant progress (every 25% of tasks completed)
                        if (completedTasks % Math.max(1, saveTasks.size() / 4) == 0 || completedTasks == saveTasks.size()) {
                            progress.setProgressPercentage(90);
                        }
                    }
                }

                progress.setCurrentPhase("Completed");
                progress.setSavedRecords(processedRecords - errorRecords);
                progress.updateProgress();

                importStatusMap.put(jobId, String.format("COMPLETED: Processed %d records, %d errors.",
                        processedRecords, errorRecords));
                logger.info("Import job {} COMPLETED. Processed {} records, {} errors.",
                        jobId, processedRecords, errorRecords);

                // Clean up duplicate counter
                duplicateCountMap.remove(jobId);
            }

        } catch (CsvValidationException e) {
            failJob(jobId, "FAILED: CSV format validation error - " + e.getMessage(), originalFilename);
        } catch (IOException e) {
            failJob(jobId, "FAILED: I/O error during file processing - " + e.getMessage(), originalFilename);
        } catch (Exception e) {
            failJob(jobId, "FAILED: An unexpected error occurred - " + e.getMessage(), originalFilename);
        }
    }
    /* This method is responsible for saving a batch (chunk) of parsed CSV records into the database asynchronously.
It is called from your CSV processing method whenever a batch of records (e.g., 5000 at a time) is ready.*/

    private Future<?> saveChunkAsync(List<AdReportData> chunk, long jobId) {
        return chunkSaveExecutor.submit(() -> {
            try {
                // Use saveAll with handling duplicate exceptions
                adReportDataRepository.saveAll(chunk);
                logger.debug("Import job {}: Saved a chunk of {} records.", jobId, chunk.size());
            } catch (org.springframework.dao.DataIntegrityViolationException e) {
                // Handle duplicates by saving one by one and catching individual conflicts
                logger.debug("Import job {}: Handling duplicates in chunk of {} records.", jobId, chunk.size());
                for (AdReportData record : chunk) {
                    try {
                        adReportDataRepository.save(record);
                    } catch (org.springframework.dao.DataIntegrityViolationException dupEx) {
                        // Find and update existing record
                        Optional<AdReportData> existing = adReportDataRepository
                                .findByMobileAppResolvedIdAndDateAndAdUnitIdAndInventoryFormatNameAndOperatingSystemVersionName(
                                        record.getMobileAppResolvedId(),
                                        record.getDate(),
                                        record.getAdUnitId(),
                                        record.getInventoryFormatName(),
                                        record.getOperatingSystemVersionName()
                                );
                        if (existing.isPresent()) {
                            updateExistingRecord(existing.get(), record);
                            adReportDataRepository.save(existing.get());
                            logger.debug("Import job {}: Updated duplicate record for App: {}, Date: {}",
                                    jobId, record.getMobileAppResolvedId(), record.getDate());
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Import job {}: Error saving chunk - {}", jobId, e.getMessage(), e);
                throw e;
            }
        });
    }

    private void updateExistingRecord(AdReportData existing, AdReportData newRecord)
    {
        // Update all non-key fields with new data (upsert behavior)
        existing.setMobileAppName(newRecord.getMobileAppName());
        existing.setDomain(newRecord.getDomain());
        existing.setAdUnitName(newRecord.getAdUnitName());
        existing.setAdExchangeTotalRequests(newRecord.getAdExchangeTotalRequests());
        existing.setAdExchangeResponsesServed(newRecord.getAdExchangeResponsesServed());
        existing.setAdExchangeMatchRate(newRecord.getAdExchangeMatchRate());
        existing.setAdExchangeLineItemLevelImpressions(newRecord.getAdExchangeLineItemLevelImpressions());
        existing.setAdExchangeLineItemLevelClicks(newRecord.getAdExchangeLineItemLevelClicks());
        existing.setAdExchangeLineItemLevelCtr(newRecord.getAdExchangeLineItemLevelCtr());
        existing.setAverageEcpm(newRecord.getAverageEcpm());
        existing.setPayout(newRecord.getPayout());
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
        List<String> expectedHeaders = List.of(
                "mobile_app_resolved_id", "mobile_app_name", "domain", "ad_unit_name", "ad_unit_id",
                "inventory_format_name", "operating_system_version_name", "date", "ad_exchange_total_requests",
                "ad_exchange_responses_served", "ad_exchange_match_rate", "ad_exchange_line_item_level_impressions",
                "ad_exchange_line_item_level_clicks", "ad_exchange_line_item_level_ctr", "average_ecpm", "payout"
        );
        if (!List.of(mappedHeader).containsAll(expectedHeaders)) {
            failJob(jobId, "FAILED: CSV header mismatch. Expected: " + expectedHeaders, filename);
            return false;
        }
        return true;
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
                    default ->
                        logger.warn("Unexpected column '{}' with value '{}'.", colName, value);
                }
            } catch (NumberFormatException | DateTimeParseException e) {
                throw new IllegalArgumentException("Data type mismatch for column '" + colName + "' with value '" + value + "'", e);
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
        } catch (InterruptedException e)
        {
            executorService.shutdownNow();
            chunkSaveExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

