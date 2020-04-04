package saga_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/thanhpd-teko/go-saga"
	"testing"
)

func initIt(mode FailureMode) {

	saga.AddSubTxDef("deduce", DeduceAccount, CompensateDeduce).
		AddSubTxDef("deposit", DepositAccount, CompensateDeposit).
		AddSubTxDef("test", PTest1, PTest1)

	memDB = map[string]int{
		"foo": 200,
		"bar": 0,
	}

	testMode = mode
}

func TestAllSuccess(t *testing.T) {

	initIt(OK)

	from, to := "foo", "bar"
	amount := 100

	ctx := context.Background()

	var sagaID uint64 = 1
	startSaga := saga.StartSaga(ctx, sagaID, saga.Memory)
	startSaga.
		ExecSub("deduce", from, amount).
		ExecSub("deposit", to, amount).
		EndSaga()

	assert.Equal(t, 100, memDB[from])
	assert.Equal(t, 100, memDB[to])

	logs, err := startSaga.GetLog().Lookup()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(logs))

}

func TestDepositFail(t *testing.T) {

	// initInStartup
	initIt(DepositFail)

	from, to := "foo", "bar"
	amount := 100

	ctx := context.Background()

	var sagaID uint64 = 1
	startSaga := saga.StartSaga(ctx, sagaID, saga.Memory)
	startSaga.
		ExecSub("deduce", from, amount).
		ExecSub("deposit", to, amount).
		EndSaga()

	// assert
	assert.Equal(t, 200, memDB[from])
	assert.Equal(t, -100, memDB[to]) // BUG fix test

	logs, err := startSaga.GetLog().Lookup()
	assert.NoError(t, err)
	t.Logf("%v", logs)
	assert.Equal(t, 0, len(logs))

}

type FailureMode int

const (
	OK = iota
	DeduceFail
	DepositFail
)

var (
	memDB    map[string]int
	testMode FailureMode
)

func DeduceAccount(ctx context.Context, account string, amount int) error {
	if testMode == DeduceFail {
		return fmt.Errorf("Deduce failure")
	}
	memDB[account] = (memDB[account] - amount)
	return nil
}

func CompensateDeduce(ctx context.Context, account string, amount int) error {
	memDB[account] = (memDB[account] + amount)
	return nil
}

func DepositAccount(ctx context.Context, account string, amount int) error {
	if testMode == DepositFail {
		return fmt.Errorf("Deposit failure")
	}
	memDB[account] = (memDB[account] + amount)
	return nil
}

func CompensateDeposit(ctx context.Context, account string, amount int) error {
	memDB[account] = (memDB[account] - amount)
	return nil
}

func PTest1(ctx context.Context, name *string, age int) {

}
