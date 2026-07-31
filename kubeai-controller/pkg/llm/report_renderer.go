package llm

import "context"

type ReportRenderer interface {
	RenderReport(ctx context.Context, raw string) (string, error)
}
