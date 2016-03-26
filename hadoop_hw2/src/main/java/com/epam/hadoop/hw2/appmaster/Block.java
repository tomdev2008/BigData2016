package com.epam.hadoop.hw2.appmaster;

import org.apache.hadoop.fs.BlockLocation;

/**
 * Created by Vitaliy on 3/26/2016.
 */
public class Block {

    private BlockLocation blockLocation;
    private String containerId;
    private BlockStatus status = BlockStatus.FREE;

    public Block(BlockLocation blockLocation) {
        this.blockLocation = blockLocation;
    }

    public BlockLocation getBlockLocation() {
        return blockLocation;
    }

    public void setBlockLocation(BlockLocation blockLocation) {
        this.blockLocation = blockLocation;
    }

    public String getContainerId() {
        return containerId;
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    public BlockStatus getStatus() {
        return status;
    }

    public void setStatus(BlockStatus status) {
        this.status = status;
    }
}
