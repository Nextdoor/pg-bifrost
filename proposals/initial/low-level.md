# Low Level Design

## Data Structures

### Wal Message

Purpose: Internal structure to hold a logical wal message which is the combination of `*pgx.WalMessage` and `*parselogical.ParseResult`.

Signature:

```
type WalMessage struct {
	msg *pgx.WalMessage
	pr  *parselogical.ParseResult
	partitionKey *string
}
```

### JsonWalEntry

Purpose: Intermediate structure used to convert a `WalMessage` into a `[]byte`.

Signature:

```
type JsonWalEntry struct {
	Time      *string                                  `json:"time"`
	Lsn       *string                                  `json:"lsn"`
	Table     *string                                  `json:"table"`
	Operation *string                                  `json:"operation"`
	Columns   *map[string]map[string]map[string]string `json:"columns"`
}
```


### Marshalled Message

Signature:

```
type MarshalledMessage struct {
	operatrion   *string
	stream       *stream
	json         []byte
	partitionKey *string
	walStart     uint64
}
```

## Components

### Replication Client

Signatures:

```
package rplclient

type client struct {
	conn       *pgx.Conn
	outputChan chan *WalMessage
	errorChan  chan error
	statsChan  chan *StatsMessage
}

/* connects to postgres and creates a new replclient */
func New(hostName string, userName string, password string, errorChan chan <- error, statsChan chan <- StatsMessage) rplclient {
	conn := postgres connection(hostName, userName, password)
	outputChan := make(chan *pgx.ReplicationMessage)
	c := rplclient(conn, outputChan, errorChan, statsChan)
	return c
}

/*
	Description: loop that reads replication slot from postgres server
	Calling: call as go routine
*/
func (c rplclient) ProcessWalMessages() {}

/*
	Description: loop that sends Standby Status back to postgres server
	Calling: call as go routine
*/
func (c rplclient) sendStatus() {}
```

### Filter

Signatures:

```
type filter struct {
	inputChan  chan *WalMessage
	outputChan chan <- *WalMessage
	errorChan  chan <- error
	statsChan  chan <- *StatsMessage
	regex      string
}

func New(regex string, inputChan chan *WalMessage, errorChan chan <- error, statsChan chan <- StatsMessage) filter {
	outputChan := make(chan *WalMessage)
	f := filter(inputChan, outputChan, errorChan, statsChan, regex)
	return f
}

/*
	Description: loop that reads wal messages off input chann, checks against table regex, and writes matches to output chan
	Calling: call as go routine
*/
func (f filter) ProcessWalMessages() {}

```

### PK Lookup

Signatures:

```
type pklookup struct {
	conn       *pgx.Conn
	inputChan  chan *WalMessage
	outputChan chan <- *WalMessage
	errorChan  chan <- error
	statsChan  chan <- *StatsMessage
	pks        sync.Map
}

func New(hostName string, userName string, password string, inputChan chan *WalMessage, errorChan chan <- error, statsChan chan <- *StatsMessage) pklookup {
	conn := postgres connection(hostName, userName, password)
	outputChan := make(chan *WalMessage)
	var pks sync.Map
	p := pklookup(conn, inputChan, outputChan, errorChan, statsChan, pks)
	return p
}

/*
	Description: loop that reads PKs from postgres and stores them
	Calling: call as go routine
*/
func (c pklookup) lookupPks() {}


/*
	Description: loop that reads wal messages off input chann, joins against PK map, and writes message to output chan
	Calling: call as go routine
*/
func (c pklookup) ProcessWalMessages() {}
```

### JSON Marshaller

Signatures:

```
type marshaller struct {
	inputChan  chan *WalMessage
	outputChan chan <- *MarshalledMessage
	errorChan  chan <- error
	statsChan  chan <- *StatsMessage
}

func New(inputChan chan <- *WalMessage, errorChan chan <- error, statsChan chan <- *StatsMessage) marshaller {
	outputChan := make(chan *WalMessage)
	m := marshaller(inputChan, outputChan, errorChan, statsChan)
	return m
}

/*
	Description: loop Marshalles WalMessage into JSON (MarshalledMessage) and writes to output chan
	Calling: call as go routine
*/
func (m marshaller) ProcessWalMessages() {}
```

### Transport Manager

Signatures:

```
type manager struct {
	inputChan    chan *MarshalledMessage
	outputChans  map[int]chan *MarshalledMessage
	errorChan    chan <- error
	statsChan    chan <- *StatsMessage
}

func New(inputChan chan <- MarshalledMessage, errorChan chan <- error, statsChan chan <- *StatsMessage) manager {

	outputChans := make(map[int]chan *putRecordEntry)
	for 1 .. n
		outputChans[i] := make(chan *MarshalledMessage)

	m := marshaller(inputChan, outputChans, errorChan, statsChan)
	return m
}

/*
	Description: loop that reads from input channel of MarshalledMessage and writes to one of the output channels
	Calling: call as go routine
*/
func (m manager) ProcessMarshalledMessages() {}
```

### Transporter

Signatures:

```
type manager transporter {
	inputChan    chan *MarshalledMessage
	errorChan    chan <- error
	statsChan    chan <- *StatsMessage
}

func New(inputChan chan <- MarshalledMessage, errorChan chan <- error, statsChan chan <- *StatsMessage) transporter {
	t := transporter(inputChan, errorChan, statsChan)
	return t
}

/*
	Description: loop that reads from input channel, buffers messages internally, and once the buffer is full writes to kinesis
	Calling: call as go routine
*/
func (m manager) ProcessMarshalledMessages() {}
```

### Stats Aggregator

Signatures:

```
type StatsMessage struct {
	component *string
	statName  *string
	count     int32
	time      uint64
	tags      map[string]string
}

type aggregator struct {
	inputChan    chan <- *StatsMessage
	outputChan   chan <- *StatsMessage
}

func New(inputChan chan <- *StatsMessage) aggregator {
	outputChan := make(chan *StatsMessage)
	a := aggregator(inputChan, outputChan)
}

/*
	Description: loop that reads from stats input channel, aggregates stats based on time and tags, and writes aggregates to output chan
	Calling: call as go routine
*/
func (a aggregator) ProcessStatsMessages() {}
```

### Stats Reporter

Signatures:

```
type aggregator struct {
	inputChan    chan <- *StatsMessage
}

func New(inputChan chan <- *StatsMessage) aggregator {
	a := aggregator(inputChan)
}

/*
	Description: loop that reads from stats input channel, formats stats message as string, and writes to stdout
	Calling: call as go routine
*/
func (a aggregator) ProcessStatsMessages() {}
```
















