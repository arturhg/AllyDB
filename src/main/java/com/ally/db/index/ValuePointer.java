package com.ally.db.index;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public final class ValuePointer {

    private String filename;

    private long lineNumber;
}
