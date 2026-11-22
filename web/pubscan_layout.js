// pubScan <https://pubscan.org> - Copyright (C) 2025-2026 Gregor Rot
// Free software under the GNU Affero General Public License, version 3 (LICENCE.md), with the additional
// attribution terms of NOTICE.md: the pubScan notice with its link stays visible in the user interface.

// pubScan: how the nodes of a network look and where they are drawn, computed before the network is drawn.
//
// network_layout(nodes, edges, center, aspect) returns for every node
//   size     radius: the area of the disc is proportional to the number of publications shared with the central author
//   font     label size: grows with the disc, so that the closest co-authors are the easiest to read
//   cluster  index of the co-author cluster of the node (largest cluster = 0), -1 for nodes outside any cluster
//   color    pastel fill color of the cluster; gray outside the seven largest clusters, red for the central author
//   outline  outline color: the strong tone of the same hue
//   x, y     position
//   label_width, label_height   room of the label, which is drawn centered below the disc
//
// Clusters are groups of co-authors who publish with each other (Louvain method on the links between co-authors,
// weighted by co-authorship weight). The positions come from a force simulation (d3-force) that is run to its end
// before anything is drawn: the network appears in place and does not move, and vis-network only draws it (its
// own physics stays switched off). Nodes and edges are the records of the get_author_network response; aspect is
// width / height of the area the network is drawn into.
//
// The drawn network is elastic: while a node is dragged, a second simulation runs in which every link is a spring
// that is relaxed at its drawn length and every node is tied to its drawn position. The nodes linked to the dragged
// one follow it, the more the stronger the link, and the rest of the network stays where it is; shortly after the
// node is dropped everything is at rest again, with the dropped node where it was put, and that shape is the
// relaxed one from then on (nothing springs back later). For this the returned object
// has drag_start(id), drag_end() and stop(); it calls on_move (set by the caller) with the positions of the nodes
// after every step and asks position_of (set by the caller) where the dragged node is.

// Cluster colors, largest cluster first: a pastel fill and an outline in the strong tone of the same hue. The outlines
// carry the distinction: every pair of them, and each of them against the red of the central author, stays apart
// for normal and for red-green deficient color vision (OKLab distance x100 at least 15 unsimulated and at least 8
// under simulated protanopia and deuteranopia); an eighth outline color would break that. The pastel fills alone are
// too close to each other for it (worst pair 9.9 and 5.2), which is why every node has an outline. Clusters beyond
// the list, and co-authors without a cluster, are gray.
const CLUSTER_COLORS = [
    { fill: "#95bceb", outline: "#2a78d6" },    // blue
    { fill: "#f6d080", outline: "#eda100" },    // amber
    { fill: "#f4bdd2", outline: "#e87ba4" },    // pink
    { fill: "#91c499", outline: "#228833" },    // green
    { fill: "#9991c4", outline: "#332288" },    // lavender
    { fill: "#b3e6f7", outline: "#66ccee" },    // sky
    { fill: "#d599bb", outline: "#aa3377" },    // orchid
];
const CLUSTER_GRAY = { fill: "#e3e4e7", outline: "#a9adb3" };
const CENTER_COLOR = { fill: "#ff0000", outline: "#b30000" };

// Layout parameters. Lengths are in layout units; a layout unit is `scale` units of the vis-network canvas, which
// keeps the node sizes in proportion to the edge widths (1-7.5) and edge labels of the network response.
const LAYOUT = {
    ticks: 200,                     // steps of the force simulation; it has cooled down completely after 85 % of them
    scale: 2,
    // disc area = shared publications x area of one publication; that unit is set per network so that
    radius_max: 32,                 // the co-author with the most shared publications gets this radius,
    radius_one: 9,                  // but one publication never more than this (networks where everybody shares 1 or 2),
    radius_min: 4,                  // and no disc is drawn smaller than this (only reached below 1/64 of the most)
    radius_center: 22,
    font_min: 16,
    font_max: 24,
    // close collaborators sit close: a spoke (central author - co-author) is long and weak, it only keeps the
    // co-author around the center; a link between two co-authors is short and strong, it forms the clusters.
    // [weakest, strongest] link of its kind
    spoke_distance: [250, 140],
    spoke_strength: [0.02, 0.08],
    link_distance: [120, 40],
    link_strength: [0.15, 0.40],
    // nodes repel each other, well-connected ones more, so that dense clusters open up
    charge_center: -3000,
    charge: -200,
    charge_per_link: -110,
    charge_per_radius: -15,
    charge_reach: 600,
    group_strength: 0.2,            // pull of a co-author towards the middle of its cluster (upper limit)
    gravity: 0.15,                  // pull towards the central author; keeps loosely attached groups from drifting far out
    gravity_shape: 2,               // how strongly the pull follows the shape of the drawing area (0: not at all)
    label_scale: 0.9,               // a label is drawn at this share of the font size its room was laid out for
    label_height: 1.2,              // height of a label line, in font sizes
    label_width: 0.5,               // average width of a character, in font sizes (when the browser cannot measure it)
    box_gap: [8, 3],                // free space kept between two nodes with their labels, horizontal and vertical
    box_strength: 0.7,
    // dragging a node
    drag_strength: 0.3,             // strength of the springs while a node is dragged (as a share of full strength)
    drag_cooling: 0.04,             // cooling per step after the node is dropped: at rest after about 140 steps
    relax_passes: 1000,             // upper limit of the passes that remove the last overlaps after a drag
    home_strength: 0.05,            // pull of a node towards its drawn position; the smaller, the further linked nodes follow
};

// Louvain community detection (Blondel et al. 2008) on a weighted undirected graph: nodes 0..n-1, links [a, b, weight].
// Returns the community of every node (numbered from 0). Deterministic: nodes are visited in their given order.
function louvain_communities(n, links) {
    let adjacency = Array.from({ length: n }, () => new Map());     // node -> (neighbour -> weight)
    let total = 0;                                                   // sum of all link weights
    for (const [a, b, w] of links) {
        if (a === b || !(w > 0)) {
            continue;
        }
        adjacency[a].set(b, (adjacency[a].get(b) || 0) + w);
        adjacency[b].set(a, (adjacency[b].get(a) || 0) + w);
        total += w;
    }
    let membership = Array.from({ length: n }, (_, i) => i);        // original node -> node of the current level
    if (total === 0) {
        return membership;
    }
    let inner = new Array(n).fill(0);                                // weight inside a node (links merged into it)
    for (let level = 0; level < 20; level++) {
        const size = adjacency.length;
        const degree = adjacency.map((neighbours, i) => {
            let sum = 2 * inner[i];
            for (const w of neighbours.values()) {
                sum += w;
            }
            return sum;
        });
        const community = Array.from({ length: size }, (_, i) => i);
        const community_degree = degree.slice();
        // local moving: a node joins the neighbouring community with the largest modularity gain, until nothing moves
        let improved = false;
        for (let pass = 0, moved = true; moved && pass < 50; pass++) {
            moved = false;
            for (let i = 0; i < size; i++) {
                const own = community[i];
                const weight_to = new Map();
                for (const [j, w] of adjacency[i]) {
                    weight_to.set(community[j], (weight_to.get(community[j]) || 0) + w);
                }
                community_degree[own] -= degree[i];
                let best = own;
                let best_gain = (weight_to.get(own) || 0) - community_degree[own] * degree[i] / (2 * total);
                for (const [c, w] of weight_to) {
                    const gain = w - community_degree[c] * degree[i] / (2 * total);
                    if (gain > best_gain + 1e-12) {
                        best_gain = gain;
                        best = c;
                    }
                }
                community_degree[best] += degree[i];
                if (best !== own) {
                    community[i] = best;
                    moved = true;
                    improved = true;
                }
            }
        }
        if (!improved) {
            break;
        }
        // aggregation: every community becomes one node of the next level
        const renumber = new Map();
        for (const c of community) {
            if (!renumber.has(c)) {
                renumber.set(c, renumber.size);
            }
        }
        const next = Array.from({ length: renumber.size }, () => new Map());
        const next_inner = new Array(renumber.size).fill(0);
        for (let i = 0; i < size; i++) {
            const ci = renumber.get(community[i]);
            next_inner[ci] += inner[i];
            for (const [j, w] of adjacency[i]) {
                const cj = renumber.get(community[j]);
                if (ci !== cj) {
                    next[ci].set(cj, (next[ci].get(cj) || 0) + w);
                } else if (i < j) {
                    next_inner[ci] += w;
                }
            }
        }
        membership = membership.map(node => renumber.get(community[node]));
        adjacency = next;
        inner = next_inner;
    }
    return membership;
}

// Width of every label at font size 1: measured in the browser, estimated from the number of characters elsewhere
function label_widths(labels) {
    let context = null;
    if (typeof document !== "undefined") {
        context = document.createElement("canvas").getContext("2d");
    }
    if (context) {
        context.font = "100px arial";
    }
    return labels.map(label => context ? context.measureText(String(label)).width / 100 : String(label).length * LAYOUT.label_width);
}

function network_layout(nodes, edges, center_id, aspect) {
    const P = LAYOUT;
    const index = new Map(nodes.map((node, i) => [node.id, i]));
    const center = index.has(center_id) ? index.get(center_id) : 0;
    aspect = Math.max(0.5, Math.min(2, aspect > 0 ? aspect : 1));

    // links as node indices: spokes (central author - co-author) and links between co-authors
    const spokes = [], links = [];
    const shared = new Array(nodes.length).fill(0);     // publications shared with the central author
    for (const edge of edges) {
        const a = index.get(edge.from), b = index.get(edge.to);
        if (a === undefined || b === undefined || a === b) {
            continue;
        }
        const weight = Math.max(edge.weight || 0, 0.0005);     // weights of huge consortium papers round to 0
        if (a === center || b === center) {
            const other = a === center ? b : a;
            shared[other] = edge.common || 0;
            spokes.push({ source: center, target: other, weight: weight });
        } else {
            links.push({ source: a, target: b, weight: weight });
        }
    }

    // node size: the area of the disc is proportional to the number of publications shared with the central author,
    // so the radius is proportional to its square root; the label grows with the radius
    const max_shared = Math.max(1, ...shared);
    const radius_one = Math.min(P.radius_max / Math.sqrt(max_shared), P.radius_one);     // radius of one shared publication
    const radius = [], font = [];
    for (let i = 0; i < nodes.length; i++) {
        radius.push(i === center ? P.radius_center : Math.max(P.radius_min, radius_one * Math.sqrt(Math.max(shared[i], 1))));
        font.push(i === center ? P.font_max : P.font_min + (P.font_max - P.font_min) * (radius[i] - P.radius_min) / (P.radius_max - P.radius_min));
    }

    // clusters of co-authors, largest first (ties: the cluster of the closer co-author first); a cluster has at least two members
    const coauthors = nodes.map((_, i) => i).filter(i => i !== center);
    const position = new Map(coauthors.map((i, k) => [i, k]));
    const membership = louvain_communities(coauthors.length, links.map(l => [position.get(l.source), position.get(l.target), l.weight]));
    const members = new Map();
    membership.forEach((community, k) => {
        if (!members.has(community)) {
            members.set(community, []);
        }
        members.get(community).push(coauthors[k]);
    });
    const groups = [...members.values()].sort((x, y) => y.length - x.length || x[0] - y[0]);
    const group = new Array(nodes.length).fill(-1);       // every co-author has a group; groups of one are not clusters
    const cluster = new Array(nodes.length).fill(-1);
    groups.forEach((list, g) => {
        for (const i of list) {
            group[i] = g;
            if (list.length >= 2) {
                cluster[i] = g;
            }
        }
    });

    // start positions: central author in the middle, every group in its own sector of a ring around it
    const x = new Array(nodes.length).fill(0), y = new Array(nodes.length).fill(0);
    let angle_start = -Math.PI / 2;
    for (const list of groups) {
        const width = 2 * Math.PI * list.length / coauthors.length;
        list.forEach((i, k) => {
            const angle = angle_start + width * (list.length > 1 ? 0.125 + 0.75 * k / (list.length - 1) : 0.5);
            const distance = 300 * (0.7 + 0.5 * ((k * 7) % 5) / 4);       // staggered: a sector is a band, not a line
            x[i] = Math.cos(angle) * distance * Math.sqrt(aspect);
            y[i] = Math.sin(angle) * distance / Math.sqrt(aspect);
        });
        angle_start += width;
    }

    // a node takes the room of its disc and of the label below it
    const widths = label_widths(nodes.map(node => node.label));
    let elastic = null;     // the simulation used while a node is dragged

    if (typeof d3 !== "undefined" && d3.forceSimulation && coauthors.length > 0) {
        const bodies = nodes.map((_, i) => {
            const label_height = P.label_height * font[i];
            return {
                x: x[i], y: y[i], r: radius[i], group: group[i], center: i === center,
                half_width: Math.max(radius[i], widths[i] * font[i] / 2) + P.box_gap[0] / 2,
                half_height: radius[i] + label_height / 2 + P.box_gap[1] / 2,
                box_offset: label_height / 2,       // the middle of the box lies below the middle of the disc
            };
        });
        bodies[center].fx = 0;      // the central author stays in the middle
        bodies[center].fy = 0;
        const degree = new Array(nodes.length).fill(0);
        for (const l of spokes.concat(links)) {
            degree[l.source]++;
            degree[l.target]++;
        }
        // weights relative to the strongest link of their kind; square root, because co-authorship weights are
        // heavy-tailed and a linear scale would make all but a few links look alike
        const max_spoke = Math.max(...spokes.map(l => l.weight), 0.0005);
        const max_link = Math.max(...links.map(l => l.weight), 0.0005);
        for (const l of spokes) {
            l.spoke = true;
            l.relative = Math.sqrt(Math.min(1, l.weight / max_spoke));
        }
        for (const l of links) {
            l.relative = Math.sqrt(Math.min(1, l.weight / max_link));
        }
        const between = (range, t) => range[0] + (range[1] - range[0]) * t;

        // every co-author is pulled towards the middle of its group, which keeps the clusters together
        const clusters = groups.filter(list => list.length > 1).length;
        const group_strength = Math.min(P.group_strength, P.group_strength / 3 * Math.sqrt(Math.max(1, clusters)));
        function group_force(alpha) {
            const sum_x = new Array(groups.length).fill(0), sum_y = new Array(groups.length).fill(0);
            for (const body of bodies) {
                if (body.group >= 0) {
                    sum_x[body.group] += body.x;
                    sum_y[body.group] += body.y;
                }
            }
            for (const body of bodies) {
                if (body.group >= 0) {
                    const size = groups[body.group].length;
                    body.vx += (sum_x[body.group] / size - body.x) * group_strength * alpha;
                    body.vy += (sum_y[body.group] / size - body.y) * group_strength * alpha;
                }
            }
        }

        // no two nodes overlap, labels included: overlapping boxes are pushed apart along the direction that needs
        // the smaller move (usually up and down, since labels are wide and flat). During the simulation the push
        // changes the velocities (strength < 1); afterwards the remaining overlaps are removed by moving the nodes.
        function separate_boxes(strength, move_nodes) {
            let overlaps = 0;
            for (let i = 0; i < bodies.length; i++) {
                const a = bodies[i];
                for (let j = i + 1; j < bodies.length; j++) {
                    const b = bodies[j];
                    const dx = move_nodes ? b.x - a.x : b.x + b.vx - a.x - a.vx;
                    const overlap_x = a.half_width + b.half_width - Math.abs(dx);
                    if (overlap_x <= 0) {
                        continue;
                    }
                    const dy = (move_nodes ? b.y - a.y : b.y + b.vy - a.y - a.vy) + b.box_offset - a.box_offset;
                    const overlap_y = a.half_height + b.half_height - Math.abs(dy);
                    if (overlap_y <= 0) {
                        continue;
                    }
                    overlaps++;
                    const share_a = a.fx != null ? 0 : b.fx != null ? 1 : 0.5;      // a fixed node does not give way
                    const horizontal = overlap_x < overlap_y;
                    const move = (horizontal ? overlap_x * (dx < 0 ? -1 : 1) : overlap_y * (dy < 0 ? -1 : 1)) * strength;
                    const key = move_nodes ? (horizontal ? "x" : "y") : (horizontal ? "vx" : "vy");
                    a[key] -= move * share_a;
                    b[key] += move * (1 - share_a);
                }
            }
            return overlaps;
        }
        function box_force() {
            for (let pass = 0; pass < 3; pass++) {
                separate_boxes(P.box_strength, false);
            }
        }

        const simulation = d3.forceSimulation(bodies)
            .stop()                                                         // no animation: all steps run below, in one go
            .alphaDecay(1 - Math.pow(0.001, 1 / (P.ticks * 0.85)))
            .velocityDecay(0.6)
            .force("link", d3.forceLink(spokes.concat(links))
                .distance(l => between(l.spoke ? P.spoke_distance : P.link_distance, l.relative))
                .strength(l => between(l.spoke ? P.spoke_strength : P.link_strength, l.relative)))
            .force("charge", d3.forceManyBody()
                .strength(body => body.center ? P.charge_center : P.charge + P.charge_per_link * degree[body.index] + P.charge_per_radius * body.r)
                .distanceMax(P.charge_reach))
            .force("group", group_force)
            // a wide drawing area gets a wide network: the pull towards the center is stronger along the short side
            .force("x", d3.forceX(0).strength(P.gravity / Math.pow(aspect, P.gravity_shape)))
            .force("y", d3.forceY(0).strength(P.gravity * Math.pow(aspect, P.gravity_shape)))
            .force("box", box_force);
        simulation.tick(P.ticks);
        const settle = (passes) => {
            for (let pass = 0; pass < passes && separate_boxes(1.02, true) > 0; pass++) {
            }
        };
        settle(300);
        bodies.forEach((body, i) => {
            x[i] = body.x;
            y[i] = body.y;
        });

        // the elastic network for dragging: springs relaxed at the drawn lengths, nodes tied to their drawn positions
        const springs = spokes.concat(links);
        for (const body of bodies) {
            body.home_x = body.x;
            body.home_y = body.y;
            body.vx = 0;
            body.vy = 0;
        }
        for (const l of springs) {      // d3 has replaced the node indices of the links by the bodies
            l.length = Math.hypot(l.source.x - l.target.x, l.source.y - l.target.y);
        }
        function home_force(alpha) {
            for (const body of bodies) {
                body.vx += (body.home_x - body.x) * P.home_strength * alpha;
                body.vy += (body.home_y - body.y) * P.home_strength * alpha;
            }
        }
        elastic = d3.forceSimulation(bodies)
            .stop()
            .alpha(0)
            .alphaDecay(P.drag_cooling)
            .velocityDecay(0.6)
            .force("link", d3.forceLink(springs)
                .distance(l => l.length)
                .strength(l => between(l.spoke ? P.spoke_strength : P.link_strength, l.relative)))
            .force("home", home_force)
            .force("box", box_force);
        elastic.bodies = bodies;
        elastic.settle = settle;
        // the shape the network has come to rest in is its relaxed shape from then on: nothing pulls back
        elastic.relax = () => {
            settle(P.relax_passes);         // a dropped node is still fixed here: the others make room for it
            for (const body of bodies) {
                if (!body.center) {         // from now on only the central author is fixed
                    body.fx = null;
                    body.fy = null;
                }
                body.home_x = body.x;
                body.home_y = body.y;
            }
            for (const l of springs) {
                l.length = Math.hypot(l.source.x - l.target.x, l.source.y - l.target.y);
            }
        };
    }

    const layout = {
        center: nodes[center].id, clusters: groups.filter(list => list.length > 1).length, nodes: {},
        elastic: elastic, on_move: null, position_of: null,
        drag_start: function () {}, drag_end: function () {}, stop: function () {},
    };
    if (elastic) {
        const bodies = elastic.bodies;
        let dragged = null;
        const report = () => {
            if (layout.on_move) {
                const positions = {};
                bodies.forEach((body, i) => {
                    if (body !== dragged) {
                        positions[nodes[i].id] = { x: body.x * P.scale, y: body.y * P.scale };
                    }
                });
                layout.on_move(positions);
            }
        };
        const follow_pointer = () => {                      // the dragged node is where the pointer has moved it
            if (dragged && layout.position_of) {
                const at = layout.position_of(nodes[dragged.index].id);
                if (at) {
                    dragged.fx = at.x / P.scale;
                    dragged.fy = at.y / P.scale;
                }
            }
        };
        elastic.on("tick", () => {
            follow_pointer();
            if (!dragged && elastic.alpha() < 0.03) {       // coming to rest: overlaps are removed step by step,
                elastic.settle(3);
            }
            report();
        });
        elastic.on("end", () => {                           // and the last ones when the network is at rest
            elastic.relax();
            report();
        });
        layout.drag_start = function (id) {
            const body = bodies[index.get(id)];
            if (!body) {
                return;
            }
            dragged = body;
            body.fx = body.x;
            body.fy = body.y;
            elastic.alphaTarget(P.drag_strength).restart();
        };
        layout.drag_end = function () {     // the node stays fixed where it was dropped while the others come to rest
            follow_pointer();
            dragged = null;
            elastic.alphaTarget(0);
        };
        layout.stop = function () {
            dragged = null;
            elastic.on("tick", null).on("end", null).stop();
        };
    }
    nodes.forEach((node, i) => {
        const colors = i === center ? CENTER_COLOR : cluster[i] >= 0 && cluster[i] < CLUSTER_COLORS.length ? CLUSTER_COLORS[cluster[i]] : CLUSTER_GRAY;
        layout.nodes[node.id] = {
            x: x[i] * P.scale,
            y: y[i] * P.scale,
            size: radius[i] * P.scale,
            font: Math.round(font[i] * P.label_scale * P.scale),
            label_width: widths[i] * font[i] * P.label_scale * P.scale,
            label_height: P.label_height * font[i] * P.label_scale * P.scale,
            shared: shared[i],
            cluster: cluster[i],
            color: colors.fill,
            outline: colors.outline,
        };
    });
    return layout;
}
