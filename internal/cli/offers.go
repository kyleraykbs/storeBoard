package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/tabwriter"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// storesCmd lists the stores peers offer this machine (DESIGN: learned
// stores) and what it does with each.
func storesCmd(ctx context.Context, configPath string, args []string, stdout, stderr io.Writer) error {
	jsonOut, err := jsonFlag(args, "stores", stderr)
	if err != nil {
		return err
	}
	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	body, err := call(ctx, cfg.Socket, http.MethodGet, "/v1/offers", nil, statusTimeout)
	if err != nil {
		return err
	}
	if jsonOut {
		_, err := stdout.Write(body)
		return err
	}
	var res struct {
		Policy string        `json:"policy"`
		Offers []api.Offered `json:"offers"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return fmt.Errorf("parse offers: %w", err)
	}
	if len(res.Offers) == 0 {
		fmt.Fprintln(stdout, "no peer has offered a store yet")
		return nil
	}
	rows := [][]string{{"STORE", "GROUP", "FROM", "HERE", "HOW"}}
	for _, o := range res.Offers {
		here := o.Carried
		if here == "" {
			here = "-"
		}
		how := o.Mode
		if !o.Subscribed && o.Carried != "declared" && o.Carried != "requested" {
			how += " (policy)"
		}
		if o.Carried == "declared" {
			how = "as the config says"
		}
		rows = append(rows, []string{o.Name, o.Group, strings.Join(o.From, ","), here, how})
	}
	table(stdout, rows)
	fmt.Fprintf(stdout, "\nnew offers: %s (learn.subscribe); storeboard subscribe NAME [--sync stream|local] to choose\n", res.Policy)
	return nil
}

// subscribeCmd chooses what to do with an offered store; unsubscribe is
// --sync off.
func subscribeCmd(ctx context.Context, configPath string, args []string, off bool, stdout, stderr io.Writer) error {
	verb := "subscribe <store> [--sync stream|local|policy]"
	if off {
		verb = "unsubscribe <store>"
	}
	jsonOut, positional, values, err := splitFlags(args, verb, []string{"--sync"}, stderr)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		fmt.Fprintf(stderr, "storeboard: usage: storeboard %s [--json]\n", verb)
		return ErrHandled
	}
	sync := values["--sync"]
	switch {
	case off:
		sync = config.SubscribeOff
	case sync == "":
		sync = config.SyncStream
	case sync == "policy":
		sync = ""
	}
	cfg, err := config.Load(config.ResolvePath(configPath))
	if err != nil {
		return err
	}
	body, err := call(ctx, cfg.Socket, http.MethodPost, "/v1/subscribe", api.Subscribe{Name: positional[0], Sync: sync}, statusTimeout)
	if err != nil {
		return err
	}
	if jsonOut {
		_, err := stdout.Write(body)
		return err
	}
	var res struct {
		Restarting bool `json:"restarting"`
	}
	json.Unmarshal(body, &res)
	what := map[string]string{"stream": "streamed here", "local": "kept here in full", "off": "not carried here", "": "following the policy again"}[sync]
	fmt.Fprintf(stdout, "%s: %s", positional[0], what)
	if res.Restarting {
		fmt.Fprint(stdout, "; the daemon is starting again to carry it")
	}
	fmt.Fprintln(stdout)
	return nil
}

func table(w io.Writer, rows [][]string) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, r := range rows {
		fmt.Fprintln(tw, strings.Join(r, "\t"))
	}
	tw.Flush()
}
