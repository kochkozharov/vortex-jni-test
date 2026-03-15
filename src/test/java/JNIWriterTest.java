// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vortex.api.DType;
import dev.vortex.api.VortexWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import dev.vortex.jni.NativeLoader;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.IntVector;
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

            // Arrow schema
            Field nameField = new Field(
                    "name",
                    FieldType.nullable(new ArrowType.Utf8()),
                    null);

            Field ageField = new Field(
                    "age",
                    FieldType.nullable(new ArrowType.Int(32, true)),
                    null);

            Schema schema = new Schema(Arrays.asList(nameField, ageField));

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

                VarCharVector name = (VarCharVector) root.getVector("name");
                IntVector age = (IntVector) root.getVector("age");

                name.allocateNew();
                age.allocateNew();

                // данные
                name.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
                age.setSafe(0, 25);

                name.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
                age.setSafe(1, 30);

                name.setValueCount(2);
                age.setValueCount(2);
                root.setRowCount(2);

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
                    "name", "age",
                },
                new DType[] {DType.newUtf8(true), DType.newInt(true)},
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
