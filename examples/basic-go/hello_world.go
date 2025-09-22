package main

type Processor struct{}

func (p Processor) ProcessLogs(logs []map[string]any) ([]any, error) {
	return nil, nil
}

func init() {
	Wire(Processor{})
}
