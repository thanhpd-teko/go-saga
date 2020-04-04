package storage

// Storage uses to support save and lookup saga log.
type Storage interface {
	// AppendLog appends log data into log under given logID
	AppendLog(data string) error

	// Lookup uses to lookup all log under given logID
	Lookup() ([]string, error)

	// Close use to close storage and release resources
	Close() error

	// Cleanup cleans up all log data in logID
	Cleanup() error

	// LastLog fetch last log entry with given logID
	LastLog() (string, error)
}
