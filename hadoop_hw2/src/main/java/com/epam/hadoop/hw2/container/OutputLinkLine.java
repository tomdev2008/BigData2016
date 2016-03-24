package com.epam.hadoop.hw2.container;

import java.util.List;

/**
 * Created by root on 3/24/16.
 */
public class OutputLinkLine {

    private InputLinkLine inputLinkLine;
    private List<String> words;

    public OutputLinkLine(InputLinkLine inputLinkLine, List<String> words) {
        this.inputLinkLine = inputLinkLine;
        this.words = words;
    }

    public InputLinkLine getInputLinkLine() {
        return inputLinkLine;
    }

    public void setInputLinkLine(InputLinkLine inputLinkLine) {
        this.inputLinkLine = inputLinkLine;
    }

    public List<String> getWords() {
        return words;
    }

    public void setWords(List<String> words) {
        this.words = words;
    }
}
