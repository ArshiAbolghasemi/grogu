package test

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/ory/dockertest/v3/docker"
)

// RunOptions defines a subset of options required for the TelC integration test harness.
type RunOptions struct {
	Repository   string
	Tag          string
	Env          []string
	Cmd          []string
	ExposedPorts []string
	PortBindings map[docker.Port][]docker.PortBinding
}

// Pool is a lightweight wrapper around docker CLI invocations.
type Pool struct {
	MaxWait time.Duration
}

// Resource represents a running container.
type Resource struct {
	ID string
}

// NewPool initializes a new Pool instance.
func NewPool(_ string) (*Pool, error) {
	return &Pool{}, nil
}

// RunWithOptions starts a container using the docker CLI.
func (p *Pool) RunWithOptions(opts *RunOptions) (*Resource, error) {
	if opts == nil {
		return nil, errors.New("nil RunOptions")
	}

	args := []string{"run", "-d"}

	for _, env := range opts.Env {
		args = append(args, "-e", env)
	}

	for _, port := range opts.ExposedPorts {
		bindings := opts.PortBindings[docker.Port(port)]
		containerPort := containerPortOnly(port)

		if len(bindings) == 0 {
			args = append(args, "-p", port)
			continue
		}

		for _, binding := range bindings {
			hostIP := binding.HostIP
			if hostIP == "" {
				hostIP = "0.0.0.0"
			}

			if binding.HostPort == "" {
				args = append(args, "-p", containerPort)
			} else {
				args = append(args, "-p", fmt.Sprintf("%s:%s:%s", hostIP, binding.HostPort, containerPort))
			}
		}
	}

	image := opts.Repository
	if opts.Tag != "" {
		image = fmt.Sprintf("%s:%s", opts.Repository, opts.Tag)
	}

	args = append(args, image)
	args = append(args, opts.Cmd...)

	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker run failed: %w (%s)", err, strings.TrimSpace(string(out)))
	}

	return &Resource{ID: strings.TrimSpace(string(out))}, nil
}

// Retry executes fn until it succeeds or MaxWait elapses.
func (p *Pool) Retry(fn func() error) error {
	if fn == nil {
		return errors.New("retry function is nil")
	}

	wait := p.MaxWait
	if wait == 0 {
		wait = 30 * time.Second
	}

	deadline := time.Now().Add(wait)

	for {
		err := fn()
		if err == nil {
			return nil
		}

		if time.Now().After(deadline) {
			return err
		}

		time.Sleep(time.Second)
	}
}

// Purge force removes the given container.
func (p *Pool) Purge(resource *Resource) error {
	if resource == nil {
		return nil
	}

	out, err := exec.Command("docker", "rm", "-f", resource.ID).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker rm failed: %w (%s)", err, strings.TrimSpace(string(out)))
	}

	return nil
}

// GetHostPort returns the host port mapping for the given container port.
func (r *Resource) GetHostPort(port string) string {
	out, err := exec.Command("docker", "port", r.ID, port).CombinedOutput()
	if err != nil {
		return ""
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 0 {
		return ""
	}

	parts := strings.Fields(lines[0])
	if len(parts) == 0 {
		return ""
	}

	return parts[len(parts)-1]
}

func containerPortOnly(port string) string {
	return strings.Split(port, "/")[0]
}
