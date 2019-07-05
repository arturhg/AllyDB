package com.ally.db.index;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@EqualsAndHashCode
public final class ValuePointer {

    private String filename;

    private long lineNumber;
}
