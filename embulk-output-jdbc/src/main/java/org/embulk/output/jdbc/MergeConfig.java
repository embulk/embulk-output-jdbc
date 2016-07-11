package org.embulk.output.jdbc;

import com.google.common.base.Optional;

import java.util.List;

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
