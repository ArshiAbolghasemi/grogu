package kafka

import (
	"github.com/xdg-go/scram"
)

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) error {
	client, err := scram.SHA512.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}

	x.Client = client
	x.ClientConversation = client.NewConversation()

	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (string, error) {
	return x.ClientConversation.Step(challenge)
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
