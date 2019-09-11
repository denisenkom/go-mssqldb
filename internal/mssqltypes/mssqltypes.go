// +build go1.9

package mssqltypes

import "time"

// VarChar parameter types.
type VarChar string

// NVarCharMax encodes parameters to NVarChar(max) SQL type.
type NVarCharMax string

// VarCharMax encodes parameter to VarChar(max) SQL type.
type VarCharMax string

// DateTime1 encodes parameters to original DateTime SQL types.
type DateTime1 time.Time

// DateTimeOffset encodes parameters to DateTimeOffset, preserving the UTC offset.
type DateTimeOffset time.Time
