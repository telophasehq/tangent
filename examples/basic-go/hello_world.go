package main

type Processor struct{}

func (p Processor) ProcessLogs(log map[string]any) error {
	return nil
}

func init() {
	Wire(Processor{})
}
