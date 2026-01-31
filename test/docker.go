package test

// Port mirrors the type signature of the real dockertest dependency.
type Port string

// PortBinding describes a host/container port mapping.
type PortBinding struct {
	HostIP   string
	HostPort string
}
