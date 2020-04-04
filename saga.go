// Package saga provide a framework for Saga-pattern to solve distribute transaction problem.
// In saga-pattern, Saga is a long-lived transaction came up with many small sub-transaction.
// ExecutionCoordinator(SEC) is coordinator for sub-transactions execute and saga-log written.
// Sub-transaction is normal business operation, it contain a Action and action's Compensate.
// Saga-Log is used to record saga process, and SEC will use it to decide next step and how to recovery from error.
//
// There is a great speak for Saga-pattern at https://www.youtube.com/watch?v=xDuwrtwYHu8
package saga

import (
	"context"
	"github.com/thanhpd-teko/go-saga/storage"
	"github.com/thanhpd-teko/go-saga/storage/memory"
	"log"
	"os"
	"reflect"
	"time"
)

const LogPrefix = "saga_"

var Logger *log.Logger

type LogProvider string

const (
	Memory LogProvider = "memory"
	Kafka  LogProvider = "kafka"
)

func GetLogStorage(_type LogProvider) storage.Storage {
	switch _type {
	case Memory:
		return memory.NewMemStorage()
	default:
		panic("unsupported provider")
	}
}

func init() {
	Logger = log.New(os.Stdout, "[Saga]", log.LstdFlags)
}

// Saga presents current execute transaction.
// A Saga constituted by small sub-transactions.
type Saga struct {
	id      uint64
	logID   string
	context context.Context
	sec     *ExecutionCoordinator
	storage storage.Storage
}

func (s *Saga) GetLog() storage.Storage {
	return s.storage
}

func (s *Saga) startSaga() {
	logger := &Log{
		Type: SagaStart,
		Time: time.Now(),
	}
	err := s.GetLog().AppendLog(logger.mustMarshal())
	if err != nil {
		panic("Add logger Failure")
	}
}

// ExecSub executes a sub-transaction for given subTxID(which define in SEC initialize) and arguments.
// it returns current Saga.
func (s *Saga) ExecSub(subTxID string, args ...interface{}) *Saga {
	subTxDef := s.sec.MustFindSubTxDef(subTxID)
	logger := &Log{
		Type:    ActionStart,
		SubTxID: subTxID,
		Time:    time.Now(),
		Params:  MarshalParam(s.sec, args),
	}
	err := s.GetLog().AppendLog(logger.mustMarshal())
	if err != nil {
		panic("Add logger Failure")
	}

	params := make([]reflect.Value, 0, len(args)+1)
	params = append(params, reflect.ValueOf(s.context))
	for _, arg := range args {
		params = append(params, reflect.ValueOf(arg))
	}
	result := subTxDef.action.Call(params)
	if isReturnError(result) {
		s.Abort()
		return s
	}

	logger = &Log{
		Type:    ActionEnd,
		SubTxID: subTxID,
		Time:    time.Now(),
	}
	err = s.GetLog().AppendLog(logger.mustMarshal())
	if err != nil {
		panic("Add logger Failure")
	}
	return s
}

// EndSaga finishes a Saga's execution.
func (s *Saga) EndSaga() {
	logger := &Log{
		Type: SagaEnd,
		Time: time.Now(),
	}
	err := s.GetLog().AppendLog(logger.mustMarshal())
	if err != nil {
		panic("Add logger Failure")
	}
	err = s.GetLog().Cleanup()
	if err != nil {
		panic("Clean up topic failure")
	}
}

// Abort stop and compensate to rollback to start situation.
// This method will stop continue sub-transaction and do Compensate for executed sub-transaction.
// SubTx will call this method internal.
func (s *Saga) Abort() {
	logs, err := s.GetLog().Lookup()
	if err != nil {
		panic("Abort Panic")
	}
	alog := &Log{
		Type: SagaAbort,
		Time: time.Now(),
	}
	err = s.GetLog().AppendLog(alog.mustMarshal())
	if err != nil {
		panic("Add log Failure")
	}
	for i := len(logs) - 1; i >= 0; i-- {
		logData := logs[i]
		logger := mustUnmarshalLog(logData)
		if logger.Type == ActionStart {
			if err := s.compensate(logger); err != nil {
				panic("Compensate Failure..")
			}
		}
	}
}

func (s *Saga) compensate(tlog Log) error {
	clog := &Log{
		Type:    CompensateStart,
		SubTxID: tlog.SubTxID,
		Time:    time.Now(),
	}
	err := s.GetLog().AppendLog(clog.mustMarshal())
	if err != nil {
		panic("Add log Failure")
	}

	args := UnmarshalParam(s.sec, tlog.Params)

	params := make([]reflect.Value, 0, len(args)+1)
	params = append(params, reflect.ValueOf(s.context))
	params = append(params, args...)

	subDef := s.sec.MustFindSubTxDef(tlog.SubTxID)
	result := subDef.compensate.Call(params)
	if isReturnError(result) {
		s.Abort()
	}

	clog = &Log{
		Type:    CompensateEnd,
		SubTxID: tlog.SubTxID,
		Time:    time.Now(),
	}
	err = s.GetLog().AppendLog(clog.mustMarshal())
	if err != nil {
		panic("Add log Failure")
	}
	return nil
}

func isReturnError(result []reflect.Value) bool {
	if len(result) == 1 && !result[0].IsNil() {
		return true
	}
	return false
}
