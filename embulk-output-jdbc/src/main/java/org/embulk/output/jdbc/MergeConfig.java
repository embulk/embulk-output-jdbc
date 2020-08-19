package org.embulk.output.jdbc;

import java.util.List;
import java.util.Optional;

public class MergeConfig {
    private final List<String> mergeKeys;
    private final Optional<List<String>> mergeRule;

    public MergeConfig(List<String> mergeKeys, Optional<List<String>> mergeRule) {
        this.mergeKeys = mergeKeys;
        this.mergeRule = mergeRule;
    }

    public List<String> getMergeKeys() {
        return mergeKeys;
    }

    public Optional<List<String>> getMergeRule() {
        return mergeRule;
    }
}
