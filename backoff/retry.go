package backoff

import (
	"context"
	"errors"
)

// Retry - повтор операции с задержкой между попытками.
//
// Сам по себе повтор без задержки бесполезен: все попытки выгорают
// за миллисекунды, пока лежит база или соседний сервис
type Retry struct {
	// Задержка между попытками
	Config

	// Сколько раз пытаться всего. 1 - без повторов
	Attempts int

	// Ошибки, на которых повторять бессмысленно: битый формат,
	// нарушение констрейнта, чужая схема.
	//
	// nil - повторяются все
	Permanent func(error) bool

	// Вызывается перед каждой задержкой: место для лога.
	// attempt - номер только что провалившейся попытки
	//
	// nil - молча
	OnRetry func(attempt int, err error)
}

// Validate проверяет конфиг и возвращает все найденные проблемы разом
func (r Retry) Validate() error {
	var errs []error

	if r.Attempts <= 0 {
		errs = append(errs, errors.New("retry: Attempts must be > 0"))
	}
	if err := r.Config.Validate(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

// Do повторяет op, пока она не пройдет или не кончатся попытки.
//
// Возвращает ошибку последней попытки. Прерывается сразу на [Retry.Permanent]
// и на отмене ctx: отмена во время ожидания - штатная остановка,
// и она приезжает вместе с исходной ошибкой
func (r Retry) Do(ctx context.Context, op func() error) error {
	var err error

	for attempt := 1; attempt <= r.Attempts; attempt++ {
		if err = op(); err == nil {
			return nil
		}

		if attempt == r.Attempts || (r.Permanent != nil && r.Permanent(err)) {
			break
		}

		if r.OnRetry != nil {
			r.OnRetry(attempt, err)
		}

		if waitErr := r.Wait(ctx, attempt); waitErr != nil {
			return errors.Join(err, waitErr)
		}
	}

	return err
}
