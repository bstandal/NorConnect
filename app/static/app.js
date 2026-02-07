/* global vis, Chart */

const graphEl = document.getElementById("graph");
const coNetworkEl = document.getElementById("co-network");
const personGraphEl = document.getElementById("person-graph");
const personKeyEl = document.getElementById("person-key");
const personBindingsEl = document.getElementById("person-bindings");
const detailsEl = document.getElementById("details");
const statsEl = document.getElementById("stats");

const qEl = document.getElementById("q");
const yearFromEl = document.getElementById("year-from");
const yearToEl = document.getElementById("year-to");
const includeRolesEl = document.getElementById("include-roles");
const includeFundingEl = document.getElementById("include-funding");
const applyEl = document.getElementById("apply");
const toplistTextEl = document.getElementById("toplist-text");

const viewButtons = Array.from(document.querySelectorAll(".view-btn"));
const viewPanes = {
  graph: document.getElementById("view-graph"),
  person: document.getElementById("view-person"),
  timeline: document.getElementById("view-timeline"),
  toplists: document.getElementById("view-toplists"),
  coboard: document.getElementById("view-coboard"),
};

const timelineRolesCanvas = document.getElementById("timeline-roles");
const timelineFundingCanvas = document.getElementById("timeline-funding");
const topOrgFundingCanvas = document.getElementById("top-org-funding");
const topPersonRolesCanvas = document.getElementById("top-person-roles");

let network;
let coNetwork;
let personNetwork;
let activeView = "graph";
let currentGraph = { nodes: [], edges: [] };
let currentCoBoard = { nodes: [], edges: [] };
let currentPersonDrilldown = { nodes: [], edges: [], bindings: [], person: null };

let timelineRolesChart;
let timelineFundingChart;
let topOrgFundingChart;
let topPersonRolesChart;

function qp(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  return String(value);
}

function escapeHtml(text) {
  const map = {
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;",
  };
  return String(text).replace(/[&<>"']/g, (m) => map[m]);
}

function sourceCard(source) {
  const name = source.source_name || "kilde";
  const relation = source.relation_type || "ref";
  const docType = source.doc_type || "document";
  const url = source.url || "";

  return `
    <div class="source">
      <div>
        <span class="chip">${escapeHtml(relation)}</span>
        <span class="chip">${escapeHtml(docType)}</span>
      </div>
      <p><b>${escapeHtml(name)}</b></p>
      <a href="${escapeHtml(url)}" target="_blank" rel="noopener">${escapeHtml(url)}</a>
    </div>
  `;
}

function metadataGrid(metadata) {
  const entries = Object.entries(metadata || {}).filter(([, v]) => v !== null && v !== "");
  if (entries.length === 0) {
    return "";
  }

  const html = entries
    .map(
      ([k, v]) => `
      <div class="meta-item">
        <b>${escapeHtml(k)}</b>
        <span>${escapeHtml(v)}</span>
      </div>
    `,
    )
    .join("\n");

  return `<div class="meta-grid">${html}</div>`;
}

function buildParams() {
  const params = new URLSearchParams();

  const q = qp(qEl.value);
  const from = qp(yearFromEl.value);
  const to = qp(yearToEl.value);

  if (q) {
    params.set("q", q);
  }
  if (from) {
    params.set("year_from", from);
  }
  if (to) {
    params.set("year_to", to);
  }

  params.set("include_roles", includeRolesEl.checked ? "true" : "false");
  params.set("include_funding", includeFundingEl.checked ? "true" : "false");

  return params;
}

function setActiveView(view) {
  activeView = view;

  for (const [key, pane] of Object.entries(viewPanes)) {
    pane.classList.toggle("active", key === view);
  }
  for (const button of viewButtons) {
    button.classList.toggle("active", button.dataset.view === view);
  }

  if (view === "graph" && network) {
    network.redraw();
  }
  if (view === "person" && personNetwork) {
    personNetwork.redraw();
  }
  if (view === "coboard" && coNetwork) {
    coNetwork.redraw();
  }
}

async function showEdgeDetails(edgeId) {
  try {
    const res = await fetch(`/api/edge/${encodeURIComponent(edgeId)}`);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    const payload = await res.json();

    const sources = (payload.sources || []).map(sourceCard).join("\n");
    const meta = metadataGrid(payload.metadata || {});

    detailsEl.innerHTML = `
      <h2>Kildepanel</h2>
      <h3>${escapeHtml(payload.title || payload.kind || "Edge")}</h3>
      <p>${escapeHtml(payload.summary || "")}</p>
      ${meta}
      <h3>Kilder (${(payload.sources || []).length})</h3>
      ${sources || "<p>Ingen kilder knyttet til denne relasjonen.</p>"}
    `;
  } catch (err) {
    detailsEl.innerHTML = `
      <h2>Kildepanel</h2>
      <p>Klarte ikke hente detaljer: ${escapeHtml(err.message)}</p>
    `;
  }
}

function showNodeDetails(nodeId) {
  const node = currentGraph.nodes.find((n) => n.id === nodeId);
  if (!node) {
    return;
  }

  const edgeCount = currentGraph.edges.filter((e) => e.from === nodeId || e.to === nodeId).length;

  detailsEl.innerHTML = `
    <h2>Kildepanel</h2>
    <h3>${escapeHtml(node.label)}</h3>
    <p>${escapeHtml(node.subtitle || node.type || "node")}</p>
    <div class="meta-grid">
      <div class="meta-item"><b>type</b><span>${escapeHtml(node.type || "")}</span></div>
      <div class="meta-item"><b>relasjoner</b><span>${edgeCount}</span></div>
    </div>
    <p>Klikk en edge for kilder og dokumentasjon.</p>
  `;
}

function showCoBoardEdgeDetails(edgeId) {
  const edge = currentCoBoard.edges.find((e) => e.id === edgeId);
  if (!edge) {
    return;
  }

  const fromNode = currentCoBoard.nodes.find((n) => n.id === edge.from);
  const toNode = currentCoBoard.nodes.find((n) => n.id === edge.to);
  const persons = (edge.person_names || []).map((p) => `<li>${escapeHtml(p)}</li>`).join("\n");
  detailsEl.innerHTML = `
    <h2>Kildepanel</h2>
    <h3>Brokoblinger mellom organisasjoner</h3>
    <p>Organisasjonsparet deler <b>${edge.shared_count}</b> person(er) med roller i begge.</p>
    <div class="meta-grid">
      <div class="meta-item"><b>fra</b><span>${escapeHtml(fromNode ? fromNode.label : edge.from)}</span></div>
      <div class="meta-item"><b>til</b><span>${escapeHtml(toNode ? toNode.label : edge.to)}</span></div>
      <div class="meta-item"><b>styrke</b><span>${edge.shared_count}</span></div>
    </div>
    <p>Delte personer:</p>
    <ul>${persons || "<li>Ingen navn tilgjengelig</li>"}</ul>
  `;
}

function formatYearSpan(startYear, endYear) {
  if (startYear && endYear) {
    return `${startYear}-${endYear}`;
  }
  if (startYear) {
    return `${startYear}-`;
  }
  if (endYear) {
    return `-${endYear}`;
  }
  return "ukjent periode";
}

function syncPersonOptions(availableProfiles, selectedKey) {
  if (!personKeyEl) {
    return;
  }
  const profiles = availableProfiles || [];
  const optionsHtml = profiles
    .map((profile) => {
      const key = profile.key || "";
      const label = profile.display_name || key;
      return `<option value="${escapeHtml(key)}">${escapeHtml(label)}</option>`;
    })
    .join("\n");

  personKeyEl.innerHTML = optionsHtml;
  if (!profiles.length) {
    return;
  }

  const current = selectedKey || personKeyEl.value || profiles[0].key;
  const match = profiles.find((profile) => profile.key === current) || profiles[0];
  personKeyEl.value = match.key;
}

function showPersonEdgeDetails(edgeId) {
  const edge = currentPersonDrilldown.edges.find((e) => e.id === edgeId);
  if (!edge) {
    return;
  }

  const fromNode = currentPersonDrilldown.nodes.find((n) => n.id === edge.from);
  const toNode = currentPersonDrilldown.nodes.find((n) => n.id === edge.to);
  const metadata = {
    ...edge.metadata,
    fra: fromNode ? fromNode.label : edge.from,
    til: toNode ? toNode.label : edge.to,
  };

  const sources = (edge.sources || []).map(sourceCard).join("\n");
  detailsEl.innerHTML = `
    <h2>Kildepanel</h2>
    <h3>${escapeHtml(edge.title || edge.label || "Binding")}</h3>
    <p>${escapeHtml((fromNode ? fromNode.label : "Person") + " -> " + (toNode ? toNode.label : ""))}</p>
    ${metadataGrid(metadata)}
    <h3>Kilder (${(edge.sources || []).length})</h3>
    ${sources || "<p>Ingen kilder registrert.</p>"}
  `;
}

function showPersonNodeDetails(nodeId) {
  const node = currentPersonDrilldown.nodes.find((n) => n.id === nodeId);
  if (!node) {
    return;
  }
  const edgeCount = currentPersonDrilldown.edges.filter(
    (edge) => edge.from === nodeId || edge.to === nodeId,
  ).length;
  const outside = node.type === "external_institution";

  detailsEl.innerHTML = `
    <h2>Kildepanel</h2>
    <h3>${escapeHtml(node.label)}</h3>
    <p>${escapeHtml(node.subtitle || node.type || "")}</p>
    <div class="meta-grid">
      <div class="meta-item"><b>type</b><span>${escapeHtml(node.type || "")}</span></div>
      <div class="meta-item"><b>relasjoner</b><span>${edgeCount}</span></div>
      <div class="meta-item"><b>utenfor datagrunnlag</b><span>${outside ? "ja" : "nei"}</span></div>
    </div>
    <p>Klikk på en binding for kilder.</p>
  `;
}

function personNodeStyle(node) {
  if (node.type === "person_focus") {
    return {
      shape: "star",
      size: 24,
      color: {
        background: "#51c9b0",
        border: "#157b6d",
      },
      font: { color: "#173a33", size: 15, face: "Space Grotesk" },
    };
  }

  if (node.type === "person_peer") {
    return {
      shape: "dot",
      size: 18,
      color: {
        background: "#92d8cc",
        border: "#2b8677",
      },
      font: { color: "#1f413a", size: 13, face: "Space Grotesk" },
    };
  }

  if (node.type === "external_institution") {
    return {
      shape: "hexagon",
      color: {
        background: "#ffe4d1",
        border: "#bf6428",
      },
      font: { color: "#4a2714", size: 12, face: "Space Grotesk" },
      margin: 8,
    };
  }

  if (node.type === "organization") {
    return {
      shape: "box",
      color: {
        background: "#f7f3ff",
        border: "#6a55a4",
      },
      font: { color: "#2f2750", size: 12, face: "Space Grotesk" },
      margin: 8,
    };
  }

  return nodeStyle(node);
}

function personEdgeStyle(edge) {
  if (edge.type === "shared_institution") {
    return {
      color: { color: "#1d6f9d", highlight: "#145274" },
      width: 1.5 + Math.min(4, Number(edge.label || 1)),
      dashes: [6, 5],
      arrows: { to: { enabled: false } },
      font: { align: "middle", size: 10, face: "IBM Plex Mono", color: "#16435d" },
    };
  }

  if (edge.type === "person_link") {
    return {
      color: { color: "#7b6752", highlight: "#5a4a3a" },
      width: 2.6,
      dashes: false,
      arrows: { to: { enabled: false } },
      font: { align: "middle", size: 10, face: "IBM Plex Mono", color: "#4f3d2f" },
    };
  }

  if (edge.source_kind === "dataset") {
    return {
      color: { color: "#188776", highlight: "#10685a" },
      width: 2.2,
      dashes: false,
      arrows: { to: { enabled: true, scaleFactor: 0.6 } },
      font: { align: "middle", size: 10, face: "IBM Plex Mono", color: "#205146" },
    };
  }

  return {
    color: { color: "#b96a2f", highlight: "#8c4f23" },
    width: 2.5,
    dashes: [10, 6],
    arrows: { to: { enabled: true, scaleFactor: 0.65 } },
    font: { align: "middle", size: 10, face: "IBM Plex Mono", color: "#6b3515" },
  };
}

function renderPersonBindings(payload) {
  if (!personBindingsEl) {
    return;
  }

  const bindings = payload.bindings || [];
  const rows = bindings
    .map((binding) => {
      const isFocus =
        currentPersonDrilldown.person &&
        currentPersonDrilldown.person.key &&
        binding.person_key === currentPersonDrilldown.person.key;
      const chips = [
        isFocus ? "Fokusperson" : "Peer",
        binding.source_kind === "dataset" ? "Datagrunnlag" : "Kuratert",
        binding.outside_dataset ? "Utenfor datagrunnlag" : "I datagrunnlag",
      ];
      const chipHtml = chips
        .map((chip) => `<span class="binding-chip">${escapeHtml(chip)}</span>`)
        .join("");
      const period = formatYearSpan(binding.start_year, binding.end_year);
      return `
        <button class="binding-item" data-edge-id="${escapeHtml(binding.id)}">
          <div class="binding-head">
            <b>${escapeHtml(binding.institution_name || "Ukjent institusjon")}</b>
            <div>${chipHtml}</div>
          </div>
          <p>${escapeHtml(binding.person_name || "Ukjent person")} · ${escapeHtml(binding.role_title || "Binding")} · ${escapeHtml(period)}</p>
        </button>
      `;
    })
    .join("\n");

  const personName = payload.person ? payload.person.display_name : "Person";
  const peopleInScope =
    payload.network_scope && payload.network_scope.people ? payload.network_scope.people.length : 1;
  personBindingsEl.innerHTML = `
    <h3>Bindinger for ${escapeHtml(personName || "person")}</h3>
    <p>${peopleInScope} person(er) i aktivt nettverk.</p>
    <div class="binding-items">
      ${rows || "<p>Ingen bindinger i valgt filter.</p>"}
    </div>
  `;

  personBindingsEl.querySelectorAll("[data-edge-id]").forEach((el) => {
    el.addEventListener("click", () => {
      const edgeId = el.getAttribute("data-edge-id");
      if (edgeId) {
        showPersonEdgeDetails(edgeId);
      }
    });
  });
}

function renderPersonDrilldown(payload) {
  currentPersonDrilldown = payload;
  syncPersonOptions(payload.available_profiles, payload.person && payload.person.key);
  renderPersonBindings(payload);

  if (!personGraphEl) {
    return;
  }

  const nodes = new vis.DataSet(
    (payload.nodes || []).map((node) => ({
      ...node,
      ...personNodeStyle(node),
    })),
  );
  const edges = new vis.DataSet(
    (payload.edges || []).map((edge) => ({
      ...edge,
      ...personEdgeStyle(edge),
    })),
  );
  const data = { nodes, edges };
  const options = {
    autoResize: true,
    interaction: {
      hover: true,
      multiselect: false,
      navigationButtons: true,
    },
    physics: {
      stabilization: { iterations: 220, fit: true },
      barnesHut: {
        gravitationalConstant: -5600,
        centralGravity: 0.18,
        springLength: 140,
        springConstant: 0.05,
        damping: 0.28,
      },
    },
  };

  if (!personNetwork) {
    personNetwork = new vis.Network(personGraphEl, data, options);
    personNetwork.on("selectEdge", (params) => {
      if (params.edges && params.edges.length > 0) {
        showPersonEdgeDetails(params.edges[0]);
      }
    });
    personNetwork.on("selectNode", (params) => {
      if (params.nodes && params.nodes.length > 0) {
        showPersonNodeDetails(params.nodes[0]);
      }
    });
  } else {
    personNetwork.setData(data);
  }
}

function nodeStyle(node) {
  if (node.type === "person") {
    return {
      shape: "dot",
      size: 14,
      color: {
        background: "#44b9a4",
        border: "#13796b",
      },
      font: { color: "#18342f", size: 13, face: "Space Grotesk" },
    };
  }

  if (node.type === "organization") {
    return {
      shape: "box",
      borderWidth: 1,
      color: {
        background: "#fff6ea",
        border: "#d27332",
      },
      margin: 7,
      font: { color: "#362418", size: 12, face: "Space Grotesk" },
    };
  }

  if (node.type === "external_recipient") {
    return {
      shape: "ellipse",
      borderWidth: 1,
      color: {
        background: "#fde8df",
        border: "#ca6a2a",
      },
      font: { color: "#4f2711", size: 12, face: "Space Grotesk" },
    };
  }

  return {
    shape: "diamond",
    size: 18,
    color: {
      background: "#d4e3ff",
      border: "#4c70bc",
    },
    font: { color: "#22304f", size: 12, face: "Space Grotesk" },
  };
}

function edgeStyle(edge) {
  if (edge.type === "role") {
    return {
      color: { color: "#188776", highlight: "#0f695c" },
      width: 1.8,
      dashes: false,
      arrows: { to: { enabled: true, scaleFactor: 0.55 } },
      font: { align: "middle", size: 11, face: "IBM Plex Mono", color: "#205146" },
    };
  }

  return {
    color: { color: "#d87a36", highlight: "#b1561a" },
    width: 2.2,
    dashes: true,
    arrows: { to: { enabled: true, scaleFactor: 0.65 } },
    font: { align: "middle", size: 10, face: "IBM Plex Mono", color: "#6b3515" },
  };
}

function renderGraph(payload) {
  currentGraph = payload;

  const nodes = new vis.DataSet(
    payload.nodes.map((n) => ({
      ...n,
      ...nodeStyle(n),
    })),
  );

  const edges = new vis.DataSet(
    payload.edges.map((e) => ({
      ...e,
      ...edgeStyle(e),
    })),
  );

  const data = { nodes, edges };
  const options = {
    autoResize: true,
    interaction: {
      hover: true,
      multiselect: false,
      navigationButtons: true,
    },
    physics: {
      stabilization: { iterations: 250, fit: true },
      barnesHut: {
        gravitationalConstant: -6200,
        centralGravity: 0.2,
        springLength: 140,
        springConstant: 0.04,
        damping: 0.25,
      },
    },
  };

  if (!network) {
    network = new vis.Network(graphEl, data, options);

    network.on("selectEdge", (params) => {
      if (params.edges && params.edges.length > 0) {
        showEdgeDetails(params.edges[0]);
      }
    });

    network.on("selectNode", (params) => {
      if (params.nodes && params.nodes.length > 0) {
        showNodeDetails(params.nodes[0]);
      }
    });
  } else {
    network.setData(data);
  }
}

function renderCoBoard(payload) {
  currentCoBoard = payload;

  const nodes = new vis.DataSet(
    payload.nodes.map((n) => ({
      ...n,
      shape: "box",
      margin: 8,
      color: {
        background: "#f8d5bb",
        border: "#b95e23",
      },
      font: { color: "#4e2a14", size: 12, face: "Space Grotesk" },
    })),
  );

  const edges = new vis.DataSet(
    payload.edges.map((e) => ({
      ...e,
      width: 1 + e.shared_count,
      color: { color: "#8f643f", highlight: "#673f1f" },
      font: { align: "middle", size: 10, face: "IBM Plex Mono", color: "#5d3920" },
    })),
  );

  const data = { nodes, edges };
  const options = {
    autoResize: true,
    interaction: {
      hover: true,
      navigationButtons: true,
    },
    physics: {
      stabilization: { iterations: 220 },
      forceAtlas2Based: {
        gravitationalConstant: -80,
        centralGravity: 0.005,
        springLength: 160,
        springConstant: 0.05,
      },
    },
  };

  if (!coNetwork) {
    coNetwork = new vis.Network(coNetworkEl, data, options);

    coNetwork.on("selectEdge", (params) => {
      if (params.edges && params.edges.length > 0) {
        showCoBoardEdgeDetails(params.edges[0]);
      }
    });
  } else {
    coNetwork.setData(data);
  }
}

function chartDefaultOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        labels: {
          font: {
            family: "IBM Plex Mono",
            size: 11,
          },
        },
      },
    },
    scales: {
      x: {
        ticks: {
          color: "#355149",
        },
        grid: {
          color: "#e4ece8",
        },
      },
      y: {
        ticks: {
          color: "#355149",
        },
        grid: {
          color: "#e4ece8",
        },
      },
    },
  };
}

function ensureChart(instance, canvas, config) {
  if (!canvas) {
    return instance;
  }
  if (instance) {
    instance.data = config.data;
    instance.options = config.options;
    instance.update();
    return instance;
  }
  return new Chart(canvas.getContext("2d"), config);
}

function toMillions(arr) {
  return (arr || []).map((v) => (v || 0) / 1_000_000);
}

function renderTimeline(payload) {
  const labels = payload.years || [];

  timelineRolesChart = ensureChart(timelineRolesChart, timelineRolesCanvas, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Nye roller",
          data: payload.role_starts || [],
          backgroundColor: "rgba(28, 138, 123, 0.65)",
          borderColor: "#1c8a7b",
          borderWidth: 1,
          yAxisID: "y",
        },
        {
          label: "Antall funding-flows",
          data: payload.funding_flows || [],
          type: "line",
          borderColor: "#d8732d",
          backgroundColor: "rgba(216, 115, 45, 0.12)",
          tension: 0.3,
          yAxisID: "y1",
        },
      ],
    },
    options: {
      ...chartDefaultOptions(),
      scales: {
        x: chartDefaultOptions().scales.x,
        y: {
          ...chartDefaultOptions().scales.y,
          position: "left",
          title: { display: true, text: "Roller" },
        },
        y1: {
          ...chartDefaultOptions().scales.y,
          position: "right",
          grid: { drawOnChartArea: false },
          title: { display: true, text: "Funding-flows" },
        },
      },
    },
  });

  timelineFundingChart = ensureChart(timelineFundingChart, timelineFundingCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "NOK (mill)",
          data: toMillions(payload.funding_nok || []),
          borderColor: "#147a6d",
          backgroundColor: "rgba(20, 122, 109, 0.18)",
          fill: true,
          tension: 0.24,
        },
        {
          label: "USD (mill)",
          data: toMillions(payload.funding_usd || []),
          borderColor: "#b44f0f",
          backgroundColor: "rgba(180, 79, 15, 0.14)",
          fill: true,
          tension: 0.24,
        },
      ],
    },
    options: chartDefaultOptions(),
  });
}

function renderToplists(payload) {
  const orgFunding = payload.org_funding_top || [];
  const personRoles = payload.person_role_top || [];

  topOrgFundingChart = ensureChart(topOrgFundingChart, topOrgFundingCanvas, {
    type: "bar",
    data: {
      labels: orgFunding.map((x) => x.org_name),
      datasets: [
        {
          label: "NOK (mill)",
          data: orgFunding.map((x) => (x.nok_total || 0) / 1_000_000),
          backgroundColor: "rgba(216, 115, 45, 0.68)",
          borderColor: "#c66320",
          borderWidth: 1,
        },
      ],
    },
    options: {
      ...chartDefaultOptions(),
      indexAxis: "y",
    },
  });

  topPersonRolesChart = ensureChart(topPersonRolesChart, topPersonRolesCanvas, {
    type: "bar",
    data: {
      labels: personRoles.map((x) => x.person_name),
      datasets: [
        {
          label: "Roller",
          data: personRoles.map((x) => x.role_count || 0),
          backgroundColor: "rgba(28, 138, 123, 0.65)",
          borderColor: "#1c8a7b",
          borderWidth: 1,
        },
      ],
    },
    options: {
      ...chartDefaultOptions(),
      indexAxis: "y",
    },
  });

  const roleOrgs = payload.org_role_top || [];
  const li = roleOrgs
    .slice(0, 8)
    .map(
      (x) =>
        `<li>${escapeHtml(x.org_name)}: ${x.role_count} roller, ${x.person_count} personer</li>`,
    )
    .join("\n");

  toplistTextEl.innerHTML = `
    <b>Organisasjoner med flest rolle-koblinger</b>
    <ul>${li || "<li>Ingen data i valgt filter</li>"}</ul>
  `;
}

function updateStats(graphStats, timelinePayload, coboardStats, personStats) {
  const s = graphStats || {};
  const years = (timelinePayload && timelinePayload.years ? timelinePayload.years.length : 0) || 0;
  const coboardEdges = (coboardStats && coboardStats.edges) || 0;
  const personEdges = (personStats && personStats.edges) || 0;
  const personSharedEdges = (personStats && personStats.shared_edges) || 0;
  const people = (personStats && personStats.people) || 0;
  const outsideInstitutions =
    (personStats && personStats.outside_dataset_institutions) || 0;
  const fundingText = s.funding_edges_truncated
    ? `${s.funding_edges || 0}/${s.funding_edges_total_matched || 0}`
    : `${s.funding_edges || 0}`;
  statsEl.textContent = `noder=${s.nodes || 0} | kanter=${s.edges || 0} | roller=${
    s.role_edges || 0
  } | funding=${fundingText} | år=${years} | co-board=${coboardEdges} | drilldown=${personEdges} | personer=${people} | delte=${personSharedEdges} | ekst-inst=${outsideInstitutions}`;
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }
  return res.json();
}

async function loadAllViews() {
  applyEl.disabled = true;
  applyEl.textContent = "Laster...";

  const params = buildParams();
  const timelineParams = new URLSearchParams(params);
  timelineParams.delete("include_roles");
  timelineParams.delete("include_funding");
  const personParams = new URLSearchParams(timelineParams);
  const personKey = qp(personKeyEl && personKeyEl.value);
  if (personKey) {
    personParams.set("person_key", personKey);
  }

  try {
    const [graphPayload, personPayload, timelinePayload, toplistsPayload, coboardPayload] =
      await Promise.all([
        fetchJson(`/api/graph?${params.toString()}`),
        fetchJson(`/api/person-drilldown?${personParams.toString()}`),
        fetchJson(`/api/timeline?${timelineParams.toString()}`),
        fetchJson(`/api/toplists?${timelineParams.toString()}`),
        fetchJson(`/api/coboard?${timelineParams.toString()}`),
      ]);

    renderGraph(graphPayload);
    renderPersonDrilldown(personPayload);
    renderTimeline(timelinePayload);
    renderToplists(toplistsPayload);
    renderCoBoard(coboardPayload);
    updateStats(graphPayload.stats, timelinePayload, coboardPayload.stats, personPayload.stats);

    if (activeView === "timeline") {
      detailsEl.innerHTML = `
        <h2>Kildepanel</h2>
        <p>Tidslinjevisningen viser utvikling i roller og finansiering per år i valgt filter.</p>
      `;
    }
    if (activeView === "person") {
      detailsEl.innerHTML = `
        <h2>Kildepanel</h2>
        <p>Person-drilldown er klar. Klikk bindinger i grafen eller listen for kilder.</p>
      `;
    }
  } catch (err) {
    detailsEl.innerHTML = `
      <h2>Kildepanel</h2>
      <p>Klarte ikke laste visualiseringene: ${escapeHtml(err.message)}</p>
    `;
  } finally {
    applyEl.disabled = false;
    applyEl.textContent = "Oppdater";
  }
}

applyEl.addEventListener("click", () => {
  loadAllViews();
});

if (personKeyEl) {
  personKeyEl.addEventListener("change", () => {
    loadAllViews();
  });
}

viewButtons.forEach((button) => {
  button.addEventListener("click", () => {
    setActiveView(button.dataset.view);
  });
});

[qEl, yearFromEl, yearToEl].forEach((el) => {
  el.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      loadAllViews();
    }
  });
});

setActiveView("graph");
loadAllViews();
