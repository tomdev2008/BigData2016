package com.epam.hadoop.hw2.appmaster;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Vitaliy on 3/26/2016.
 */
public class FileBlocks {

    private List<Block> blocks;

    public FileBlocks() {
        blocks = new LinkedList<>();
    }

    public void addBlock(Block block) {
        blocks.add(block);
    }

    public synchronized Block findFreeBlock(String host) {
        return blocks.stream()
                .filter(block -> presentsOnHost(block, host))
                .filter(block -> BlockStatus.FREE.equals(block.getStatus()))
                .findFirst()
                .get();
    }

    private boolean presentsOnHost(Block block, String host) {
        try {
            return Arrays.asList(block.getBlockLocation().getHosts())
                    .stream()
                    .anyMatch(blockHost -> blockHost.equals(host));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
