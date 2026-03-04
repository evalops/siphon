package poller

type CheckpointStore interface {
	Get(provider string) (string, bool)
	Set(provider, checkpoint string)
}
