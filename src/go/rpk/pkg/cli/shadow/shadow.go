// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"context"
	"errors"
	"fmt"
	"time"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	maxOpRetries = 5
	retryDelay   = 2500 // milliseconds
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "shadow",
		Args:  cobra.NoArgs,
		Short: "Manage Redpanda Shadow Links",
		Long: `Manage Redpanda Shadow Links.

Shadowing is Redpanda's enterprise-grade disaster recovery solution that
establishes asynchronous, offset-preserving replication between two distinct
Redpanda clusters. A cluster is able to create a dedicated client that
continuously replicates source cluster data, including offsets, timestamps, and
cluster metadata.
`,
	}
	cmd.AddCommand(
		newShadowConfigCommand(fs, p),
		newCreateCommand(fs, p),
		newDeleteCommand(fs, p),
		newFailoverCommand(fs, p),
		newDescribeCommand(fs, p),
		newStatusCommand(fs, p),
		newListCommand(fs, p),
		newUpdateCommand(fs, p),
	)
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	return cmd
}

// waitForOperation is a shared function to poll for the completion of an async
// Shadow Link operation. (e.g., create, delete, update).
func waitForOperation(ctx context.Context, cloudClient *publicapi.CloudClientSet, opID string) (isCompleted bool, err error) {
	backoff := func(i int) {
		sleepTime := retryDelay * (1 << i) // Exponential backoff.
		zap.L().Sugar().Debugf("Shadow Link operation not completed yet, retrying in %d ms", sleepTime)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
	for i := range maxOpRetries {
		shadowLink, err := cloudClient.Operations.GetOperation(ctx, connect.NewRequest(&controlplanev1.GetOperationRequest{
			Id: opID,
		}))
		if err != nil {
			if i < maxOpRetries-1 {
				zap.L().Sugar().Debugf("unable to get Shadow Link Operation %q, retrying: %v", opID, err)
				backoff(i)
				continue
			}
			return false, fmt.Errorf("unable to get Shadow Link Operation: %v", err)
		}
		switch shadowLink.Msg.GetOperation().GetState() {
		case controlplanev1.Operation_STATE_COMPLETED:
			return true, nil
		case controlplanev1.Operation_STATE_FAILED:
			return false, errors.New("an error occurred while attempting to perform the operation.\nPlease run the command again or contact Redpanda Cloud Support")
		default:
			if i < maxOpRetries-1 {
				backoff(i)
				continue
			}
			return false, nil
		}
	}
	return false, nil
}
