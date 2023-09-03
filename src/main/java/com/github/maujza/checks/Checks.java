package com.github.maujza.checks;


import com.github.maujza.exceptions.ConfigException;

import java.util.function.Predicate;
import java.util.function.Supplier;

/** Assertions to validate inputs */
public final class Checks {

    public static void ensureState(
            final Supplier<Boolean> stateCheck, final Supplier<String> errorMessageSupplier) {
        if (!stateCheck.get()) {
            throw new IllegalStateException(errorMessageSupplier.get());
        }
    }

    private Checks() {}
}
