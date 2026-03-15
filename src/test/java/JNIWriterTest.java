// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vortex.api.DType;
import dev.vortex.api.VortexWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.vortex.jni.NativeLoader;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.*;

/**
 * Direct test of JNI writer to isolate pointer alignment issue.
 */
public final class JNIWriterTest {

    static Path tempDir = Path.of(System.getProperty("java.io.tmpdir"));

    @BeforeAll
    public static void loadLibrary() {
        NativeLoader.loadJni();
    }

    public static byte[] createArrowBatch() throws Exception {

        try (RootAllocator allocator = new RootAllocator()) {

            // 1) Определяем поля
            Field nameField = new Field(
                    "Name",
                    FieldType.nullable(new ArrowType.Utf8()),
                    null
            );

            Field salaryField = new Field(
                    "Salary",
                    FieldType.nullable(new ArrowType.Decimal(10, 2)),
                    null
            );

            Field stateField = new Field(
                    "State",
                    FieldType.nullable(new ArrowType.Utf8()),
                    null
            );

            Schema schema = new Schema(Arrays.asList(nameField, salaryField, stateField));

            // 2) Создаем VectorSchemaRoot
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

                VarCharVector name = (VarCharVector) root.getVector("Name");
                DecimalVector salary = (DecimalVector) root.getVector("Salary");
                VarCharVector state = (VarCharVector) root.getVector("State");

                name.allocateNew();
                salary.allocateNew();
                state.allocateNew();

                // 3) Данные
                List<String> names = Arrays.asList(
                        "Alice", "Bob", "Carol", "Dan", "Edward",
                        "Frida", "George", "Henry", "Ida", "John"
                );

                List<BigDecimal> salaries = Arrays.asList(
                        new BigDecimal("10.00"),
                        new BigDecimal("20.00"),
                        new BigDecimal("30.00"),
                        new BigDecimal("40.00"),
                        new BigDecimal("50.00"),
                        new BigDecimal("60.00"),
                        new BigDecimal("70.00"),
                        new BigDecimal("80.00"),
                        new BigDecimal("90.00"),
                        new BigDecimal("100.00")
                );

                List<String> states = Arrays.asList(
                        "CA", "NY", "TX", "CA", "NY",
                        "TX", "CA", "NY", "TX", "VA"
                );

                for (int i = 0; i < names.size(); i++) {
                    name.setSafe(i, names.get(i).getBytes(StandardCharsets.UTF_8));
                    salary.setSafe(i, salaries.get(i));
                    state.setSafe(i, states.get(i).getBytes(StandardCharsets.UTF_8));
                }

                name.setValueCount(names.size());
                salary.setValueCount(names.size());
                state.setValueCount(names.size());
                root.setRowCount(names.size());

                // 4) Сериализуем в Arrow IPC
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }

                return out.toByteArray();
            }
        }
    }

    @Test
    public void testCreateWriter() throws IOException {
        // Test just creating and closing a writer
        Path outputPath = tempDir.resolve("test_create.vortex");
        String writePath = outputPath.toAbsolutePath().toUri().toString();

        // Make a new file writer with a very simple schema.
        var writeSchema = DType.newStruct(
                new String[] {
                    "Name", "Salary", "State"
                },
                new DType[] {DType.newUtf8(true), DType.newDecimal(10, 2, true), DType.newUtf8(true)},
                false);

        // Minimal Arrow schema
        Map<String, String> options = new HashMap<>();

        System.err.println("Creating writer for path: " + writePath);

        try (VortexWriter writer = VortexWriter.create(writePath, writeSchema, options)) {
            assertNotNull(writer);
            System.err.println("Writer created successfully");
            writer.writeBatch(createArrowBatch());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Verify file was created
        assertTrue(Files.exists(outputPath), "Output file should exist");
        System.err.println("File created at: " + outputPath);

    }
}
