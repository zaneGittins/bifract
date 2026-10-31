package query

import (
	"sort"
	"strings"

	"bifract/pkg/parser"
)

const (
	// reconLeadMinEdgeScore is how anomalous the seed's own edge to a shared artifact must be
	// (its edge_score, before diffusion) for the link to be a lead.
	reconLeadMinEdgeScore = 0.8
	// reconLeadMaxArtifacts: a tree process sharing more artifacts than this with other trees
	// reads as fleet software (an agent and its telemetry), not one specific bridge.
	reconLeadMaxArtifacts = 3
	// reconLeadMaxBridges caps the artifacts promoted as leads, strongest first.
	reconLeadMaxBridges = 3
)

// Reconnection roles, carried in the recon_role column of bridge rows.
const (
	reconRoleLead    = "lead"    // another host reached through an artifact the seed had no business touching
	reconRoleShared  = "shared"  // part of a wide family of shared artifacts: shared software
	reconRoleContext = "context" // everything else
)

// classifyReconnections sets recon_role on the bridge rows in rows. A link is a lead when the
// peer is on another host, the seed's own edge to the shared artifact is anomalous (a dropped
// file another host ran always is), and the tree process does not share many artifacts. The
// bridge score alone cannot tell these apart: a fleet agent's telemetry domains are as rare and
// as new as a C2.
func classifyReconnections(rows []map[string]interface{}, peers []parser.ReconnectPeer) {
	if len(peers) == 0 {
		return
	}
	hostOf := map[string]string{}
	ownScore := map[string]float64{}
	for _, r := range rows {
		et, parent, child := reconString(r["event_type"]), reconString(r["parent"]), reconString(r["child"])
		switch {
		case et == "spawn":
			if h := reconString(r["host"]); h != "" {
				hostOf[child] = h
			}
		case reconString(r["score_basis"]) != "reconnect":
			if h := reconString(r["host"]); h != "" && hostOf[parent] == "" {
				hostOf[parent] = h
			}
			ownScore[parent+"\x00"+child] = reconFloat(r["edge_score"])
		}
	}

	artifact := func(pe parser.ReconnectPeer) string {
		if pe.ObjectID != "" {
			return pe.ObjectID
		}
		return pe.ReconType + "\x00" + pe.Label
	}
	family := map[string]map[string]bool{}
	for _, pe := range peers {
		if pe.SrcGUID == "" {
			continue
		}
		if family[pe.SrcGUID] == nil {
			family[pe.SrcGUID] = map[string]bool{}
		}
		family[pe.SrcGUID][artifact(pe)] = true
	}

	type candidate struct {
		key     string
		anomaly float64
		hosts   map[string]bool
	}
	roleOf := map[int]string{}
	cands := map[string]*candidate{}
	for i, pe := range peers {
		if pe.SrcGUID == "" {
			continue // injection/access: no bridge row of its own
		}
		key := artifact(pe)
		cross := pe.PeerHost != "" && hostOf[pe.SrcGUID] != "" && !strings.EqualFold(pe.PeerHost, hostOf[pe.SrcGUID])
		sharp := pe.ReconType == "file" || ownScore[pe.SrcGUID+"\x00"+pe.ObjectID] >= reconLeadMinEdgeScore
		switch {
		case len(family[pe.SrcGUID]) > reconLeadMaxArtifacts:
			roleOf[i] = reconRoleShared
		case cross && sharp:
			roleOf[i] = reconRoleLead
			c := cands[key]
			if c == nil {
				c = &candidate{key: key, hosts: map[string]bool{}}
				cands[key] = c
			}
			c.anomaly = max(c.anomaly, pe.Anomaly)
			c.hosts[strings.ToLower(pe.PeerHost)] = true
		default:
			roleOf[i] = reconRoleContext
		}
	}
	ranked := make([]*candidate, 0, len(cands))
	for _, c := range cands {
		ranked = append(ranked, c)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].anomaly != ranked[j].anomaly {
			return ranked[i].anomaly > ranked[j].anomaly
		}
		if len(ranked[i].hosts) != len(ranked[j].hosts) {
			return len(ranked[i].hosts) > len(ranked[j].hosts)
		}
		return ranked[i].key < ranked[j].key
	})
	kept := map[string]bool{}
	for i, c := range ranked {
		kept[c.key] = i < reconLeadMaxBridges
	}

	// A row takes the strongest role of the peers it carries: the tree-side edge of a net/dns
	// bridge is shared by every peer of that artifact.
	rank := map[string]int{reconRoleShared: 1, reconRoleContext: 2, reconRoleLead: 3}
	byRow := map[string]string{}
	set := func(parent, child, role string) {
		k := parent + "\x00" + child
		if rank[role] > rank[byRow[k]] {
			byRow[k] = role
		}
	}
	for i, pe := range peers {
		role, ok := roleOf[i]
		if !ok {
			continue
		}
		if role == reconRoleLead && !kept[artifact(pe)] {
			role = reconRoleContext
		}
		if pe.ObjectID != "" {
			set(pe.SrcGUID, pe.ObjectID, role)
			set(pe.PeerGUID, pe.ObjectID, role)
		} else {
			set(pe.SrcGUID, pe.PeerGUID, role)
		}
	}
	for _, r := range rows {
		if reconString(r["score_basis"]) != "reconnect" {
			continue
		}
		if role, ok := byRow[reconString(r["parent"])+"\x00"+reconString(r["child"])]; ok {
			r["recon_role"] = role
		}
	}
}
