/*
	Source from:
		- (original) https://github.com/nickelser/parselogical
		- (fork)     https://github.com/Nextdoor/parselogical
*/

package parselogical

import (
	"strings"

	"github.com/pkg/errors"
)

const (
	parseStateInitial int = iota // start State to determine the message type

	// prelude section
	parseStateRelation          // <schema>.<table>
	parseStateOperation         // INSERT/UPDATE/DELETE
	parseStateEscapedIdentifier // applicable to both the column names, and the relation for when "" is used
	parseStateOperationTruncate // a terminal operation state

	// name and type of the column values
	parseStateColumnName
	parseStateColumnType
	parseStateOpenSquareBracket

	// value parsing
	parseStateColumnValue
	parseStateColumnQuotedValue

	// terminal states
	parseStateEnd
	parseStateNull
)

// ColumnValue is an annotated String type, containing the Postgres type of the String,
// and whether it was quoted within the original parsing.
// Quoting is useful for handling values like val[text]:null or val[text]:unchanged-toast-datum,
// which are special signifiers (denoting null and unchanged data, respectively).
type ColumnValue struct {
	Value  string
	Type   string
	Quoted bool
}

type ParseState struct {
	Msg *string

	Current       int
	Prev          int
	TokenStart    int
	OldKey        bool
	CurColumnName string
	CurColumnType string
}

// ParseResult is the result of parsing the string
type ParseResult struct {
	State ParseState // for internal use

	Transaction string                 // filled if this is a transaction statement (BEGIN/COMMIT) with the value of the transaction
	Relation    string                 // filled if this is a DML statement with <schema>.<table>
	Operation   string                 // filled if this is a DML statement with the name of the operation (INSERT/UPDATE/DELETE)
	NoTupleData bool                   // true if this had no actual tuple data
	Columns     map[string]ColumnValue // if a DML statement, the fields affected by the operation with their values and types
	OldColumns  map[string]ColumnValue // if an UPDATE and REPLICA IDENTITY setting (FULL or USING INDEX both will add values here)
}

// NewParseResult creates the structure that is filled by the Parse operation
func NewParseResult(msg string) *ParseResult {
	pr := new(ParseResult)
	pr.State = ParseState{Msg: &msg, Current: parseStateInitial, Prev: parseStateInitial, TokenStart: 0, OldKey: false}
	pr.Columns = make(map[string]ColumnValue)
	pr.OldColumns = make(map[string]ColumnValue)
	return pr
}

// Parse parses a string produced from the test_decoding logical replication plugin into the ParseResult struct, above
func (pr *ParseResult) Parse() error {
	err := pr.parse(false)
	if err != nil {
		return err
	}
	return nil
}

// ParsePrelude allows more control over the parsing process
// if you want to avoid parsing the string for tables/schemas (via a filtering mechansim)
// you can run ParsePrelude to just fill the operation type, the schema and the table
func (pr *ParseResult) ParsePrelude() error {
	return pr.parse(true)
}

// ParseColumns also does some cool shit
func (pr *ParseResult) ParseColumns() error {
	return pr.parse(false)
}

// based on https://github.com/citusdata/pg_warp/blob/9e69814b85e18fbe5a3c89f0e17d22583c9a398c/consumer/consumer.go
// as opposed to a hackier earlier version
func (pr *ParseResult) parse(preludeOnly bool) error {
	state := pr.State
	message := *state.Msg

	if state.Current == parseStateInitial {
		if len(message) < 5 {
			return errors.Errorf("message too short: %s", message)
		}

		switch message[0:5] {
		case "BEGIN":
			fallthrough
		case "COMMI":
			fields := strings.Fields(message)

			if len(fields) != 2 {
				return errors.Errorf("unknown transaction message: %s", message)
			}

			pr.Operation = fields[0]
			pr.Transaction = fields[1]

			return nil
		case "table":
			// actual DML statement for which we need to parse out the table/operation
		default:
			return errors.Errorf("unknown logical message received: %s", message)
		}

		// we are parsing a table statement, so let's skip over the initial "table "
		state.TokenStart = 6
		state.Current = parseStateRelation
	}

outer:
	for i := 0; i <= len(message); i++ {
		if i < state.TokenStart {
			i = state.TokenStart - 1
			continue
		}

		chr := byte('\000')
		if i < len(message) {
			chr = message[i]
		}

		chrNext := byte('\000')
		if i+1 < len(message) {
			chrNext = message[i+1]
		}

		switch state.Current {
		case parseStateNull:
			return errors.Errorf("invalid parse State null: %+v", state)
		case parseStateRelation:
			if chr == ':' {
				if chrNext != ' ' {
					return errors.Errorf("invalid character ' ' at %d", i+1)
				}
				pr.Relation = message[state.TokenStart:i]
				state.TokenStart = i + 2
				state.Current = parseStateOperation
			} else if chr == '"' {
				state.Prev = state.Current
				state.Current = parseStateEscapedIdentifier
			}
		case parseStateOperation:
			if chr == ':' {
				if chrNext != ' ' {
					return errors.Errorf("invalid character ' ' at %d", i+1)
				}
				pr.Operation = message[state.TokenStart:i]
				if pr.Operation == "TRUNCATE" {
					state.Current = parseStateOperationTruncate
					break outer
				}

				state.TokenStart = i + 2
				state.Current = parseStateColumnName

				if preludeOnly {
					break outer
				}
			}
		case parseStateColumnName:
			if chr == '[' {
				state.CurColumnName = message[state.TokenStart:i]
				state.TokenStart = i + 1
				state.Current = parseStateColumnType
			} else if chr == ':' {
				if message[state.TokenStart:i] == "old-key" {
					state.OldKey = true
				} else if message[state.TokenStart:i] == "new-tuple" {
					state.OldKey = false
				}
				state.TokenStart = i + 2
			} else if chr == '(' && message[state.TokenStart:] == "(no-tuple-data)" {
				pr.NoTupleData = true
				state.Current = parseStateEnd
			} else if chr == '"' {
				state.Prev = state.Current
				state.Current = parseStateEscapedIdentifier
			}
		case parseStateColumnType:
			if chr == ']' {
				if chrNext != ':' {
					return errors.Errorf("invalid character '%s' at %d", []byte{chrNext}, i+1)
				}
				state.CurColumnType = message[state.TokenStart:i]
				state.TokenStart = i + 2
				state.Current = parseStateColumnValue
			} else if chr == '"' {
				state.Prev = state.Current
				state.Current = parseStateEscapedIdentifier
			} else if chr == '[' {
				state.Prev = state.Current
				state.Current = parseStateOpenSquareBracket
			}
		case parseStateColumnValue:
			if chr == '\000' || chr == ' ' {
				quoted := state.Prev == parseStateColumnQuotedValue
				startStr := state.TokenStart
				endStr := i

				if quoted {
					startStr++
					endStr--
				}

				unescapedValue := strings.Replace(message[startStr:endStr], "''", "'", -1) // the value is escape-quoted to us

				cv := ColumnValue{Value: unescapedValue, Quoted: quoted, Type: state.CurColumnType}

				if state.OldKey {
					pr.OldColumns[state.CurColumnName] = cv
				} else {
					pr.Columns[state.CurColumnName] = cv
				}
			}

			if chr == '\000' {
				state.Current = parseStateEnd
			} else if chr == ' ' {
				state.TokenStart = i + 1
				state.Prev = state.Current
				state.Current = parseStateColumnName
			} else if chr == '\'' {
				state.Prev = state.Current
				state.Current = parseStateColumnQuotedValue
			}
		case parseStateOpenSquareBracket:
			if chr == ']' {
				state.Current = state.Prev
				state.Prev = parseStateNull
			}
		case parseStateEscapedIdentifier:
			if chr == '"' {
				if chrNext == '"' {
					i++
				} else {
					state.Current = state.Prev
					state.Prev = parseStateNull
				}
			}
		case parseStateColumnQuotedValue:
			if chr == '\'' {
				if chrNext == '\'' {
					i++
				} else {
					prev := state.Prev
					state.Prev = state.Current
					state.Current = prev
				}
			}
		}
	}

	if state.Current == parseStateOperationTruncate {
		return nil
	}

	if (preludeOnly && state.Current != parseStateColumnName) || (!preludeOnly && state.Current != parseStateEnd) {
		return errors.Errorf("invalid parser end State: %+v", state.Current)
	}

	return nil
}
