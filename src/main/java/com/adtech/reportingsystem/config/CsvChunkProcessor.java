package com.adtech.reportingsystem.config;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;

@Component
public class CsvChunkProcessor {

    /**
     * Reads a CSV from InputStream and processes it in chunks.
     *
     * @param csvStream     Input CSV stream
     * @param chunkSize     Number of rows per chunk
     * @param headerHandler Consumer for header mapping
     * @param chunkHandler  Consumer for processing a list of String[] rows
     */
    public void processInChunks(InputStream csvStream,
                                int chunkSize,
                                Consumer<String[]> headerHandler,
                                Consumer<List<String[]>> chunkHandler)
            throws IOException, CsvValidationException {

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(csvStream));
             CSVReader csvReader = new CSVReader(reader)) {

            // Read and handle header
            String[] header = csvReader.readNext();
            if (header == null) {
                throw new IllegalArgumentException("Empty CSV: No header found.");
            }
            headerHandler.accept(header);

            // Read data rows in chunks
            List<String[]> chunk = new ArrayList<>();
            String[] row;
            while ((row = csvReader.readNext()) != null) {
                chunk.add(row);
                if (chunk.size() >= chunkSize) {
                    chunkHandler.accept(new ArrayList<>(chunk));
                    chunk.clear();
                }
            }

            // Last remaining rows
            if (!chunk.isEmpty()) {
                chunkHandler.accept(chunk);
            }
        }
    }
}