package com.jovisco.tutorials.reactive.sec10;

import java.io.BufferedWriter;
import java.nio.file.Path;

public class FileWriter {

    private final Path path;
    private BufferedWriter writer;

    private FileWriter(Path path) {
        this.path = path;
    }
}
