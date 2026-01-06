package htcondor

import (
	"context"
	"fmt"
	"github.com/golang/groupcache"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/retzkek/htcondor-go/classad"
)

var (
	// CommandDuration is a prometheus histogram metric that records the
	// duration to run each command. It is up to the client to register this
	// metric with the prometheus client, e.g.
	//
	//    func init() {
	//        prometheus.MustRegister(htcondor.CommandDuration)
	//    }
	CommandDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "htcondor_client_command_duration_seconds",
			Help: "Histogram of command runtimes.",
		},
		[]string{"command"},
	)
	tracer = otel.Tracer("htcondor")
)

const (
	keySeparator    = "\x1f"     // unit separator
	attributeFormat = "-af:lrng" // format command for condor attributes
)

// Command represents an HTCondor command-line tool, e.g. condor_q.
//
// It implements a builder pattern, so you can call e.g.
//
//	NewCommand("condor_q").WithPool("mypool:9618").WithName("myschedd").WithConstraint("Owner == \"Me\"")
//
// You can also build it directly, e.g.
//
//	c := Command{
//	   Command: "condor_q",
//	   Pool: "mypool:9618",
//	   Name: "myschedd",
//	   Constraint: "Owner == \"Me\"",
//	}
type Command struct {
	// Command is HTCondor command to run.
	Command string
	// Pool is HTCondor pool (collector) to query.
	Pool string
	// Name is the -name argument.
	Name string
	// Limit is the -limit argument.
	Limit int
	// Constraint sets the -constraint argument.
	Constraint string
	// Attributes is a list of specific attributes to return.
	// If Attributes is empty, all attributes are returned.
	Attributes []string
	// Args is a list of any extra arguments to pass.
	Args []string
	// Env is the environment the command will run in.
	// Env is similar to exec.Cmd.Env, namely:
	// - Each entry is of the form "key=value".
	// - If Env is nil, the Command uses the current process's
	// environment.
	// - If Env contains duplicate key-value strings, the last will be used
	Env []string
	// cache is an optional groupcache pool to cache
	// queries. Inititalize with WithCache().
	cache         *groupcache.HTTPPool
	cacheGroup    string
	cacheLifetime time.Duration
}

// NewCommand creates a new HTCondor command.
func NewCommand(command string) *Command {
	return &Command{
		Command: command,
	}
}

// Copy returns a new copy of the command, useful for adding further arguments
// without changing the base command. The commands share the cache.
func (c *Command) Copy() *Command {
	cc := Command{
		Command:       c.Command,
		Pool:          c.Pool,
		Name:          c.Name,
		Limit:         c.Limit,
		Constraint:    c.Constraint,
		Attributes:    make([]string, len(c.Attributes)),
		Args:          make([]string, len(c.Args)),
		Env:           make([]string, len(c.Env)),
		cache:         c.cache,
		cacheGroup:    c.cacheGroup,
		cacheLifetime: c.cacheLifetime,
	}
	if len(c.Attributes) > 0 {
		copy(cc.Attributes, c.Attributes)
	}
	if len(c.Args) > 0 {
		copy(cc.Args, c.Args)
	}
	if len(c.Env) > 0 {
		copy(cc.Env, c.Env)
	}
	return &cc
}

// WithCache initializes a groupcache group for the client. Set cacheLifetime to
// 0 to *never* expire cached queries (unless they are LRU evicted).
func (c *Command) WithCache(pool *groupcache.HTTPPool, group string, cacheBytes int64, cacheLifetime time.Duration) *Command {
	c.cache = pool
	c.cacheGroup = group
	c.cacheLifetime = cacheLifetime
	if groupcache.GetGroup(group) == nil {
		groupcache.NewGroup(c.cacheGroup, cacheBytes, commandGetter())
	}
	return c
}

// WithPool sets the -pool argument for the command.
func (c *Command) WithPool(pool string) *Command {
	c.Pool = pool
	return c
}

// WithName sets the -name argument for the command.
func (c *Command) WithName(name string) *Command {
	c.Name = name
	return c
}

// WithLimit sets the -limit argument for the command.
func (c *Command) WithLimit(limit int) *Command {
	c.Limit = limit
	return c
}

// WithConstraint set the -constraint argument for the command.
func (c *Command) WithConstraint(constraint string) *Command {
	c.Constraint = constraint
	return c
}

// WithAttribute sets a specific attribute to return, rather than the entire
// ClassAd. Can be called multiple times.
func (c *Command) WithAttribute(attribute string) *Command {
	if c.Attributes == nil {
		c.Attributes = []string{attribute}
	} else {
		c.Attributes = append(c.Attributes, attribute)
	}
	return c
}

// WithArg adds an extra argument to pass. Can be called multiple times.
func (c *Command) WithArg(arg string) *Command {
	if c.Args == nil {
		c.Args = []string{arg}
	} else {
		c.Args = append(c.Args, arg)
	}
	return c
}

// WithEnv sets the environment in which the command will run.
func (c *Command) WithEnv(env []string) *Command {
	c.Env = env
	return c
}

// MakeArgs builds the complete argument list to be passed to the command.
func (c *Command) MakeArgs() []string {
	args := make([]string, 0)
	if c.Pool != "" {
		args = append(args, "-pool", c.Pool)
	}
	if c.Name != "" {
		args = append(args, "-name", c.Name)
	}
	if c.Limit > 0 {
		args = append(args, "-limit", fmt.Sprintf("%d", c.Limit))
	}
	if c.Constraint != "" {
		args = append(args, "-constraint", c.Constraint)
	}
	if len(c.Args) > 0 {
		args = append(args, c.Args...)
	}
	if len(c.Attributes) > 0 {
		args = append(args, attributeFormat)
		args = append(args, c.Attributes...)
	} else {
		args = append(args, "-long")
	}
	return args
}

// Cmd generates an exec.Cmd you can use to run the command manually.
// Use Run() to run the command and get back ClassAds.
func (c *Command) Cmd() *exec.Cmd {
	cmd := exec.Command(c.Command, c.MakeArgs()...)
	cmd.Env = c.Env
	return cmd
}

// CmdContext generates an exec.Cmd with context you can use to run the command
// manually. Use Run() to run the command and get back ClassAds.
func (c *Command) CmdContext(ctx context.Context) *exec.Cmd {
	cmd := exec.CommandContext(ctx, c.Command, c.MakeArgs()...)
	cmd.Env = c.Env
	return cmd
}

// encodeKey encodes the command into a string, to be used as a cache key.
func (c *Command) encodeKey() string {
	timeKey := "0"
	if c.cacheLifetime > 0 {
		timeKey = time.Now().Truncate(c.cacheLifetime).Format(time.RFC3339)
	}
	return timeKey + keySeparator +
		c.Command + keySeparator +
		strings.Join(c.MakeArgs(), keySeparator)
}

// decodeKey decodes the command from a key string. It does not restore the
// original Command, instead putting all the arguments into Args.
func decodeKey(key string) (*Command, error) {
	parts := strings.Split(key, keySeparator)
	if len(parts) < 2 {
		return nil, fmt.Errorf("unable to decode cache key: %s", key)
	}
	// first field is time key, we don't need it
	c := Command{
		Command: parts[1],
	}
	if len(parts) > 2 {
		endArgs := len(parts) - 1
		for i, arg := range parts {
			if arg == attributeFormat {
				endArgs = i
				break
			}
		}
		c.Args = parts[2:endArgs]
		if endArgs < len(parts)-1 {
			c.Attributes = parts[endArgs+1:]
		}
	}
	return &c, nil
}

// commandGetter returns a groupCache.GetterFunc that queries HTCondor with the
// configured command, and stores the raw response in dest.
func commandGetter() groupcache.GetterFunc {
	return func(ctx context.Context, key string, dest groupcache.Sink) error {
		ctx, span := tracer.Start(ctx, "Getter")
		defer span.End()
		span.SetAttributes(attribute.String("key", key))

		c, err := decodeKey(key)
		if err != nil {
			err := fmt.Errorf("error decoding key: %w", err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		c.addTracingTags(span)
		timer := prometheus.NewTimer(CommandDuration.WithLabelValues(c.Command))
		defer timer.ObserveDuration()

		cmd := c.CmdContext(ctx)
		out, err := cmd.StdoutPipe()
		if err != nil {
			err := fmt.Errorf("error creating stdout pipe: %w", err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			err := fmt.Errorf("error creating stderr pipe: %w", err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		if err := cmd.Start(); err != nil {
			err := fmt.Errorf("error creating command: %w", err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		resp, err := io.ReadAll(out)
		if err != nil {
			err := fmt.Errorf("error reading stdout: %w", err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		rerr, err := io.ReadAll(stderr)
		if err != nil {
			err := fmt.Errorf("error reading stderr: %w", err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		if err := cmd.Wait(); err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(
				attribute.String("stdout", string(resp)),
				attribute.String("stderr", string(rerr)),
			)
			return err
		}
		return dest.SetBytes(resp)
	}
}

// Run runs the command and returns the ClassAds.
// Use Cmd() if you need more control over the handling of the output.
func (c *Command) Run() ([]classad.ClassAd, error) {
	return c.RunWithContext(context.Background())
}

// RunWithContext runs the command with the given context and returns the ClassAds. Use
// Cmd() if you need more control over the handling of the output.
func (c *Command) RunWithContext(ctx context.Context) ([]classad.ClassAd, error) {
	ctx, span := tracer.Start(ctx, "Run")
	defer span.End()
	c.addTracingTags(span)

	key := c.encodeKey()
	var resp groupcache.ByteView
	var err error
	if c.cache != nil {
		group := groupcache.GetGroup(c.cacheGroup)
		err = group.Get(ctx, key, groupcache.ByteViewSink(&resp))
	} else {
		// call the getter directly
		err = commandGetter()(ctx, key, groupcache.ByteViewSink(&resp))
	}
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	ads, err := classad.ReadClassAds(resp.Reader())
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return ads, nil
}

// Stream runs the command and sends the ClassAds on a channel. Errors are
// returned on a separate channel. Both will be closed when the command is done.
//
// N.B. if using Stream with a cache you'll lose much of performance and memory
// advantages of streaming, since the entire HTCondor response must be read,
// whether from HTCondor or from the cache, before the classads can be sent.
func (c *Command) Stream(ch chan classad.ClassAd, errors chan error) {
	c.StreamWithContext(context.Background(), ch, errors)
}

// StreamWithContext runs the command with the given context and sends the
// ClassAds on a channel. Errors are returned on a separate channel. Both will
// be closed when the command is done.
//
// N.B. if using Stream with a cache you'll lose much of performance and memory
// advantages of streaming, since the entire HTCondor response must be read,
// whether from HTCondor or from the cache, before the classads can be sent.
func (c *Command) StreamWithContext(ctx context.Context, ch chan classad.ClassAd, errors chan error) {
	ctx, span := tracer.Start(ctx, "Stream")
	defer span.End()
	c.addTracingTags(span)

	if c.cache != nil {
		key := c.encodeKey()
		var resp groupcache.ByteView
		var err error
		group := groupcache.GetGroup(c.cacheGroup)
		err = group.Get(ctx, key, groupcache.ByteViewSink(&resp))
		if err != nil {
			err = fmt.Errorf("error getting response from cache: %w", err)
			span.SetStatus(codes.Error, err.Error())
			errors <- err
			close(errors)
			close(ch)
			return
		}
		classad.StreamClassAds(resp.Reader(), ch, errors)
	} else {
		cmd := c.CmdContext(ctx)
		out, err := cmd.StdoutPipe()
		if err != nil {
			err = fmt.Errorf("error opening command pipe: %w", err)
			span.SetStatus(codes.Error, err.Error())
			errors <- err
			close(errors)
			close(ch)
			return
		}
		if err := cmd.Start(); err != nil {
			err = fmt.Errorf("error running command: %w", err)
			span.SetStatus(codes.Error, err.Error())
			errors <- err
			close(errors)
			close(ch)
			return
		}
		classad.StreamClassAds(out, ch, errors)
		cmd.Wait()
	}
}

func (c *Command) addTracingTags(span trace.Span) {
	span.SetAttributes(attribute.String("component", "htcondor"))
	span.SetAttributes(attribute.String("db.type", "htcondor"))
	span.SetAttributes(attribute.String("db.instance", c.Pool))
	span.SetAttributes(attribute.String("db.statement", c.Command+" "+strings.Join(c.MakeArgs(), " ")))
}
