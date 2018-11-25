package com.blog.stream.pagination.demo;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class Exporter {

    private static final Logger LOG = LoggerFactory.getLogger(Exporter.class);

    private final CsvSchema schema;
    private final CsvMapper csvMapper;
    private final OutputStream outputStream;

    private Exporter(final CsvSchema schema, final CsvMapper csvMapper, final OutputStream outputStream) {
        this.schema = schema;
        this.csvMapper = csvMapper;
        this.outputStream = outputStream;
    }

    public static Exporter create(final OutputStream outputStream) {
        CsvMapper objectMapper = new CsvMapper();
        objectMapper.registerModules(new JavaTimeModule());
        objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

        CsvSchema schema = objectMapper.schemaFor(UserExport.class);

        return new Exporter(schema, objectMapper, outputStream);
    }

    public synchronized void exportUser(final UserExport userExport) {
        try {
            LOG.info("Writing user export {} to file", userExport.getFirstName());
            csvMapper.writer().with(schema).writeValue(outputStream, userExport);
        } catch (IOException e) {
            throw new IllegalStateException("Im being lazy here, just kill it if something goes wrong");
        }
    }

}
