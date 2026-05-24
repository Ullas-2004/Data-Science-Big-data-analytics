const $ = selector => document.querySelector(selector);

const refs = {
  datasetSelect: $("#dataset-select"),
  runButton: $("#run-button"),
  jobPill: $("#job-pill"),
  jobMessage: $("#job-message"),
  jobProgress: $("#job-progress"),
  datasetRail: $("#dataset-rail"),
  summaryGrid: $("#summary-grid"),
  agreementRing: $("#agreement-ring"),
  runtimeDuel: $("#runtime-duel"),
  pagerankBars: $("#pagerank-bars"),
  differenceDeck: $("#difference-deck"),
  timingChart: $("#timing-chart"),
  networkNote: $("#network-note"),
  view2d: $("#view-2d"),
  view3d: $("#view-3d"),
  networkStage: $("#network-stage"),
  networkStage3d: $("#network-stage-3d"),
  metricsSummary: $("#metrics-summary"),
  metricsToggle: $("#metrics-toggle"),
  metricsDrawer: $("#metrics-drawer"),
  metricsTable: $("#metrics-table"),
  pagerankOverlap: $("#pagerank-overlap"),
  pagerankGrid: $("#pagerank-grid"),
  runCards: $("#run-cards"),
  researchGrid: $("#research-grid"),
  gfLogPath: $("#gf-log-path"),
  gxLogPath: $("#gx-log-path"),
  gfLog: $("#gf-log"),
  gxLog: $("#gx-log"),
  modal: $("#proof-modal"),
  closeProof: $("#close-proof"),
  proofTitle: $("#proof-title"),
  proofNote: $("#proof-note"),
  proofSummary: $("#proof-summary"),
  proofGfPath: $("#proof-gf-path"),
  proofGxPath: $("#proof-gx-path"),
  proofGfPreview: $("#proof-gf-preview"),
  proofGxPreview: $("#proof-gx-preview"),
};

const state = {
  datasets: [],
  results: new Map(),
  payload: null,
  selectedSlug: null,
  activeJob: null,
  pollTimer: null,
  previewCache: new Map(),
  networkCache: new Map(),
  currentNetworkPreview: null,
  graphView: "2d",
  orbitFrame: null,
};

const escapeHtml = value => String(value ?? "")
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;")
  .replaceAll("'", "&#39;");

const formatSize = value => value === null || value === undefined ? "N/A" : `${Number(value).toFixed(2)} MB`;
const formatTime = value => value === null || value === undefined ? "N/A" : `${Number(value).toFixed(2)} s`;
const statusClass = status => ({
  matched: "matched",
  different: "different",
  "framework-specific": "framework-specific",
  completed: "ok",
  running: "warn live",
  queued: "neutral live",
  failed: "bad",
  ok: "ok",
  bad: "bad",
  warn: "warn",
  neutral: "neutral",
}[status] || "neutral");

const apiGet = async path => {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
};

const apiPost = async (path, body) => {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || `Request failed: ${response.status}`);
  }
  return data;
};

const cardHtml = (label, value, note) => `
  <article class="summary-card">
    <p class="eyebrow">${escapeHtml(label)}</p>
    <strong>${escapeHtml(value)}</strong>
    <span>${escapeHtml(note)}</span>
  </article>
`;

const proofButtonHtml = metricIndex => `
  <button class="ghost" data-proof-index="${metricIndex}">Open Proof</button>
`;

const datasetActionButton = (label, action, slug) => `
  <button class="ghost" data-action="${action}" data-slug="${escapeHtml(slug)}">${escapeHtml(label)}</button>
`;

const emptyStateHtml = message => `
  <div class="empty-state">
    <div>${escapeHtml(message)}</div>
  </div>
`;

async function bootstrap() {
  bindEvents();
  await refreshDatasets();
}

function bindEvents() {
  refs.datasetSelect.addEventListener("change", async event => {
    state.selectedSlug = event.target.value;
    await loadSelectedDataset();
  });

  refs.runButton.addEventListener("click", async () => {
    if (!state.selectedSlug) {
      return;
    }
    try {
      const job = await apiPost("/api/run", { dataset: state.selectedSlug });
      state.activeJob = job;
      syncJobUi(job);
      startPolling(job.id);
    } catch (error) {
      refs.jobMessage.textContent = error.message;
      refs.jobPill.className = "pill bad";
      refs.jobPill.textContent = "Error";
    }
  });

  refs.datasetRail.addEventListener("click", async event => {
    const button = event.target.closest("button[data-action]");
    if (!button) {
      return;
    }
    const slug = button.dataset.slug;
    state.selectedSlug = slug;
    refs.datasetSelect.value = slug;
    if (button.dataset.action === "run") {
      refs.runButton.click();
      return;
    }
    await loadPayload(slug);
  });

  refs.metricsTable.addEventListener("click", handleProofClick);
  refs.differenceDeck.addEventListener("click", handleProofClick);
  refs.metricsToggle.addEventListener("click", toggleMetricsDrawer);
  refs.view2d.addEventListener("click", () => setGraphView("2d"));
  refs.view3d.addEventListener("click", () => setGraphView("3d"));
  refs.closeProof.addEventListener("click", closeModal);
  refs.modal.addEventListener("click", event => {
    if (event.target === refs.modal) {
      closeModal();
    }
  });
  document.addEventListener("keydown", event => {
    if (event.key === "Escape") {
      closeModal();
    }
  });
}

function setGraphView(mode) {
  if (mode !== "3d") {
    stopOrbitAnimation();
  }
  state.graphView = mode;
  refs.view2d.classList.toggle("active", mode === "2d");
  refs.view3d.classList.toggle("active", mode === "3d");
  refs.networkStage.classList.toggle("hidden-stage", mode !== "2d");
  refs.networkStage3d.classList.toggle("hidden-stage", mode !== "3d");
  if (mode === "3d" && state.currentNetworkPreview) {
    renderNetwork3d(state.currentNetworkPreview);
  }
}

function stopOrbitAnimation() {
  if (state.orbitFrame) {
    cancelAnimationFrame(state.orbitFrame);
    state.orbitFrame = null;
  }
}

function toggleMetricsDrawer(forceOpen = null) {
  const shouldOpen = forceOpen === null
    ? !refs.metricsDrawer.classList.contains("is-open")
    : Boolean(forceOpen);
  refs.metricsDrawer.classList.toggle("is-open", shouldOpen);
  refs.metricsToggle.classList.toggle("is-open", shouldOpen);
  refs.metricsToggle.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
  refs.metricsToggle.querySelector(".toggle-label").textContent = shouldOpen
    ? "Close Difference Table"
    : "Open Difference Table";
}

async function refreshDatasets() {
  const data = await apiGet("/api/datasets");
  state.datasets = data.datasets || [];
  state.results = new Map((data.results || []).map(item => [item.slug, item]));
  populateDatasetSelect();
  renderDatasetRail();

  const preferred = state.selectedSlug
    || (state.datasets.find(item => item.has_result) || state.datasets[0] || {}).slug
    || null;

  state.selectedSlug = preferred;
  if (preferred) {
    refs.datasetSelect.value = preferred;
    await loadSelectedDataset();
  } else {
    renderEmptyDashboard("No datasets were found in data_lake.");
  }
}

function populateDatasetSelect() {
  refs.datasetSelect.innerHTML = state.datasets.map(dataset => `
    <option value="${escapeHtml(dataset.slug)}">
      ${escapeHtml(dataset.name)} (${escapeHtml(dataset.file_name)})
    </option>
  `).join("");
}

function renderDatasetRail() {
  if (!state.datasets.length) {
    refs.datasetRail.innerHTML = emptyStateHtml("Add SNAP edge-list files to the data_lake folder to begin.");
    return;
  }

  refs.datasetRail.innerHTML = state.datasets.map(dataset => {
    const active = dataset.slug === state.selectedSlug ? "active" : "";
    const latest = dataset.latest_result;
    const runtimeCard = latest?.hero_cards?.find(card => card.label === "Measured Runtime Gap");
    const loadLabel = dataset.has_result ? "Load Saved Proof" : "No Saved Proof Yet";
    return `
      <article class="dataset-card ${active}">
        <div class="run-head">
          <div>
            <h3>${escapeHtml(dataset.name)}</h3>
            <div class="muted">${escapeHtml(dataset.file_name)}</div>
          </div>
          <span class="pill ${dataset.has_result ? "ok" : "neutral"}">${dataset.has_result ? "Saved" : "New"}</span>
        </div>
        <div class="dataset-meta">
          <span>Size: ${escapeHtml(formatSize(dataset.size_mb))}</span>
          <span>${dataset.has_result ? `Last result: ${escapeHtml(latest.generated_at || "Saved")}` : "No saved result yet"}</span>
          <span>${runtimeCard ? `Runtime gap: ${escapeHtml(runtimeCard.value)}` : "Run this dataset to measure latency"}</span>
        </div>
        <div class="dataset-actions">
          ${dataset.has_result ? datasetActionButton(loadLabel, "load", dataset.slug) : ""}
          ${datasetActionButton("Run Dataset", "run", dataset.slug)}
        </div>
      </article>
    `;
  }).join("");
}

async function loadSelectedDataset() {
  if (!state.selectedSlug) {
    return;
  }
  const dataset = state.datasets.find(item => item.slug === state.selectedSlug);
  if (dataset?.has_result) {
    await loadPayload(state.selectedSlug);
  } else {
    state.payload = null;
    renderEmptyDashboard(`No saved comparison exists for ${dataset?.name || "this dataset"} yet. Click "Run Selected Dataset" to generate one.`);
  }
  renderDatasetRail();
}

async function loadPayload(slug) {
  const payload = await apiGet(`/api/results/${encodeURIComponent(slug)}`);
  state.payload = payload;
  renderDashboard();
}

function renderEmptyDashboard(message) {
  refs.summaryGrid.innerHTML = emptyStateHtml(message);
  refs.agreementRing.innerHTML = emptyStateHtml("Agreement visualization will appear with a loaded result.");
  refs.runtimeDuel.innerHTML = emptyStateHtml("Runtime duel will appear with a loaded result.");
  refs.pagerankBars.innerHTML = emptyStateHtml("PageRank bar view will appear with a loaded result.");
  refs.differenceDeck.innerHTML = emptyStateHtml("Differences will appear here after a saved result is loaded.");
  refs.timingChart.innerHTML = emptyStateHtml("Latency charts need a completed run.");
  refs.networkNote.textContent = "A lightweight preview built from the selected SNAP edge list.";
  refs.networkStage.innerHTML = emptyStateHtml("Network visualization will appear after a dataset is selected.");
  refs.networkStage3d.innerHTML = emptyStateHtml("3D graph visualization will appear after a dataset is selected.");
  state.currentNetworkPreview = null;
  stopOrbitAnimation();
  setGraphView("2d");
  refs.metricsSummary.textContent = "Click the button to open a simpler animated table of only the important differences.";
  toggleMetricsDrawer(false);
  refs.metricsTable.innerHTML = `<tr><td colspan="5">${escapeHtml(message)}</td></tr>`;
  refs.pagerankOverlap.textContent = "";
  refs.pagerankGrid.innerHTML = emptyStateHtml("PageRank proof will appear after a run completes.");
  refs.runCards.innerHTML = emptyStateHtml("Execution details are not available yet.");
  refs.researchGrid.innerHTML = emptyStateHtml("Research-backed differences appear with a loaded result.");
  refs.gfLogPath.textContent = "";
  refs.gxLogPath.textContent = "";
  refs.gfLog.textContent = "";
  refs.gxLog.textContent = "";
}

function renderDashboard() {
  const payload = state.payload;
  if (!payload) {
    return;
  }

  const dataset = payload.dataset || {};
  const summaryCards = [
    ...(payload.hero_cards || []),
    {
      label: "Dataset",
      value: dataset.name || "N/A",
      note: `${dataset.file_name || "Unknown file"} | ${formatSize(dataset.size_mb)}`,
    },
    {
      label: "Saved Proof Bundle",
      value: dataset.slug || "N/A",
      note: payload.generated_at ? `Generated on ${payload.generated_at}` : "Generated locally",
    },
  ];

  refs.summaryGrid.innerHTML = summaryCards.map(card => cardHtml(card.label, card.value, card.note)).join("");
  renderMiniVisuals(payload);
  renderDifferenceDeck(payload);
  renderTimings(payload);
  renderNetwork(payload);
  renderMetrics(payload);
  renderPagerank(payload);
  renderRunCards(payload);
  renderResearch(payload);
  renderLogs(payload);
  renderDatasetRail();
}

function renderMiniVisuals(payload) {
  renderAgreementRing(payload);
  renderRuntimeDuel(payload);
  renderPagerankBars(payload);
}

function renderAgreementRing(payload) {
  const metrics = payload.metrics || [];
  const comparable = metrics.filter(row => row.match !== null);
  const matched = comparable.filter(row => row.match === true).length;
  const different = comparable.length - matched;
  const total = Math.max(comparable.length, 1);
  const radius = 58;
  const circumference = 2 * Math.PI * radius;
  const matchedDash = (matched / total) * circumference;
  const differentDash = circumference - matchedDash;

  refs.agreementRing.innerHTML = `
    <div class="ring-wrap">
      <svg width="160" height="160" viewBox="0 0 160 160" role="img" aria-label="Metric agreement ring">
        <circle cx="80" cy="80" r="${radius}" fill="none" stroke="rgba(255,255,255,0.08)" stroke-width="16"></circle>
        <circle cx="80" cy="80" r="${radius}" fill="none" stroke="rgba(78,225,177,0.95)" stroke-width="16"
          stroke-linecap="round" stroke-dasharray="${matchedDash} ${circumference}" transform="rotate(-90 80 80)"></circle>
        <circle cx="80" cy="80" r="${radius}" fill="none" stroke="rgba(255,125,141,0.9)" stroke-width="16"
          stroke-linecap="round" stroke-dasharray="${differentDash} ${circumference}" stroke-dashoffset="-${matchedDash}"
          transform="rotate(-90 80 80)"></circle>
        <text x="80" y="76" text-anchor="middle" fill="#edf6ff" font-size="30" font-family="Bahnschrift, Trebuchet MS, sans-serif">${matched}</text>
        <text x="80" y="98" text-anchor="middle" fill="#9cb5c8" font-size="12" font-family="Bahnschrift, Trebuchet MS, sans-serif">matched</text>
      </svg>
      <div class="ring-copy">
        <strong>${matched} / ${comparable.length}</strong>
        <span>Comparable metrics aligned across both frameworks.</span>
        <span>${different} row${different === 1 ? "" : "s"} still need explanation during the demo.</span>
      </div>
    </div>
  `;
}

function renderRuntimeDuel(payload) {
  const totalRow = (payload.timings || []).find(row => row.algorithm === "TOTAL");
  if (!totalRow) {
    refs.runtimeDuel.innerHTML = emptyStateHtml("No total runtime row was found.");
    return;
  }

  const gf = Number(totalRow.graphframes || 0);
  const gx = Number(totalRow.graphx || 0);
  const maxValue = Math.max(gf, gx, 1);
  const winner = gf && gx
    ? (gf < gx ? "GraphFrames" : "GraphX")
    : "N/A";
  const gap = totalRow.speedup ? `${Number(totalRow.speedup).toFixed(2)}x` : "N/A";

  refs.runtimeDuel.innerHTML = `
    <div class="duel-wrap">
      <div class="duel-head">
        <div>
          <strong>${escapeHtml(winner)}</strong>
          <div class="muted">faster on total runtime</div>
        </div>
        <span class="pill ${winner === "GraphX" ? "ok" : winner === "GraphFrames" ? "warn" : "neutral"}">${escapeHtml(gap)}</span>
      </div>
      <div class="duel-bars">
        <div class="duel-row">
          <div class="lane-label"><span>GraphFrames</span><span>${formatTime(gf)}</span></div>
          <div class="mini-track"><div class="mini-fill gf" style="width:${(gf / maxValue) * 100}%"></div></div>
        </div>
        <div class="duel-row">
          <div class="lane-label"><span>GraphX</span><span>${formatTime(gx)}</span></div>
          <div class="mini-track"><div class="mini-fill gx" style="width:${(gx / maxValue) * 100}%"></div></div>
        </div>
      </div>
    </div>
  `;
}

function renderPagerankBars(payload) {
  const pagerank = payload.pagerank || {};
  const buildRows = (rows, klass) => {
    const topRows = (rows || []).slice(0, 5);
    const maxScore = Math.max(1, ...topRows.map(row => Number(row.score || 0)));
    return topRows.map(row => `
      <div class="barset-row">
        <div class="barset-label"><span>${escapeHtml(row.node)}</span><span>${escapeHtml(row.score)}</span></div>
        <div class="mini-track"><div class="mini-fill ${klass}" style="width:${(Number(row.score || 0) / maxScore) * 100}%"></div></div>
      </div>
    `).join("");
  };

  refs.pagerankBars.innerHTML = `
    <div class="barset-grid">
      <div class="barset">
        <div class="barset-title">GraphFrames Top 5</div>
        ${buildRows(pagerank.graphframes, "gf")}
      </div>
      <div class="barset">
        <div class="barset-title">GraphX Top 5</div>
        ${buildRows(pagerank.graphx, "gx")}
      </div>
    </div>
  `;
}

function metricRows(payload, mode = "difference") {
  const rows = payload.metrics || [];
  if (mode === "all") {
    return rows.map((row, index) => ({ ...row, metricIndex: index }));
  }
  return rows
    .map((row, index) => ({ ...row, metricIndex: index }))
    .filter(row => row.match !== true);
}

function renderDifferenceDeck(payload) {
  const metrics = payload.metrics || [];
  const diffRows = metrics
    .map((row, index) => ({ ...row, metricIndex: index }))
    .filter(row => row.match !== true);

  if (!diffRows.length) {
    refs.differenceDeck.innerHTML = cardHtml(
      "All comparable rows aligned",
      "No direct disagreements",
      "Framework-specific metrics can still exist even when shared metrics align."
    );
    return;
  }

  refs.differenceDeck.innerHTML = diffRows.map(row => `
    <article class="difference-card">
      <div class="topline">
        <h3>${escapeHtml(row.label)}</h3>
        <span class="status-tag ${statusClass(row.proof.status)}">${escapeHtml(row.proof.status)}</span>
      </div>
      <div class="difference-values">
        <span>GraphFrames: <strong>${escapeHtml(row.graphframes)}</strong></span>
        <span>GraphX: <strong>${escapeHtml(row.graphx)}</strong></span>
      </div>
      <p class="difference-note">${escapeHtml(row.proof.note)}</p>
      ${proofButtonHtml(row.metricIndex)}
    </article>
  `).join("");
}

function renderTimings(payload) {
  const timings = payload.timings || [];
  if (!timings.length) {
    refs.timingChart.innerHTML = emptyStateHtml("No timing rows were found.");
    return;
  }

  const maxTime = Math.max(
    1,
    ...timings.map(item => Number(item.graphframes || 0)),
    ...timings.map(item => Number(item.graphx || 0))
  );

  refs.timingChart.innerHTML = timings.map(item => `
    <article class="timing-row">
      <div class="timing-head">
        <strong>${escapeHtml(item.algorithm)}</strong>
        <span class="muted">${item.speedup ? `${Number(item.speedup).toFixed(2)}x GF slower` : "No direct speedup computed"}</span>
      </div>
      <div class="timing-lanes">
        <div>
          <div class="lane-label"><span>GraphFrames</span><span>${escapeHtml(formatTime(item.graphframes))}</span></div>
          <div class="lane"><div class="lane-fill gf" style="width:${Math.max((Number(item.graphframes || 0) / maxTime) * 100, item.graphframes ? 2 : 0)}%"></div></div>
        </div>
        <div>
          <div class="lane-label"><span>GraphX</span><span>${escapeHtml(formatTime(item.graphx))}</span></div>
          <div class="lane"><div class="lane-fill gx" style="width:${Math.max((Number(item.graphx || 0) / maxTime) * 100, item.graphx ? 2 : 0)}%"></div></div>
        </div>
      </div>
    </article>
  `).join("");
}

function renderMetrics(payload) {
  const metrics = metricRows(payload, "difference");
  if (!metrics.length) {
    refs.metricsSummary.textContent = "No direct differences were found for this dataset. The drawer stays available in case you still want to discuss framework-specific metrics from the cards above.";
    refs.metricsTable.innerHTML = `<tr><td colspan="5">No direct disagreements in the shared metrics for this dataset.</td></tr>`;
    toggleMetricsDrawer(false);
    return;
  }

  refs.metricsSummary.textContent = `${metrics.length} focused row${metrics.length === 1 ? "" : "s"} can be opened as a simple difference table with proof buttons.`;
  refs.metricsTable.innerHTML = metrics.map(row => `
    <tr>
      <td><strong>${escapeHtml(row.label)}</strong></td>
      <td>${escapeHtml(row.graphframes)}</td>
      <td>${escapeHtml(row.graphx)}</td>
      <td><span class="status-tag ${statusClass(row.proof.status)}">${escapeHtml(row.proof.status)}</span></td>
      <td>${proofButtonHtml(row.metricIndex)}</td>
    </tr>
  `).join("");
}

async function renderNetwork(payload) {
  const slug = payload?.dataset?.slug;
  if (!slug) {
    refs.networkStage.innerHTML = emptyStateHtml("Network preview unavailable.");
    return;
  }

  refs.networkNote.textContent = "Rendering a sampled node-link snapshot from the selected SNAP file.";
  refs.networkStage.innerHTML = emptyStateHtml("Loading sampled graph preview...");
  refs.networkStage3d.innerHTML = emptyStateHtml("Building 3D graph orbit...");
  stopOrbitAnimation();

  try {
    let preview = state.networkCache.get(slug);
    if (!preview) {
      preview = await apiGet(`/api/network/${encodeURIComponent(slug)}`);
      state.networkCache.set(slug, preview);
    }
    state.currentNetworkPreview = preview;

    const width = 980;
    const height = 360;
    const cx = width / 2;
    const cy = height / 2;
    const nodes = (preview.nodes || []).slice(0, 70);
    const edges = preview.edges || [];
    const maxDegree = Math.max(1, ...nodes.map(node => Number(node.degree || 1)));
    const positions = new Map();

    nodes.forEach((node, index) => {
      const angle = (index / Math.max(nodes.length, 1)) * Math.PI * 2;
      const radialPull = 1 - (Number(node.degree || 0) / maxDegree) * 0.36;
      const rx = 310 * radialPull;
      const ry = 118 + (1 - radialPull) * 54;
      positions.set(node.id, {
        x: cx + Math.cos(angle) * rx,
        y: cy + Math.sin(angle) * ry,
        r: 4 + (Number(node.degree || 0) / maxDegree) * 8,
        degree: Number(node.degree || 0),
      });
    });

    const edgeSvg = edges
      .filter(edge => positions.has(edge.source) && positions.has(edge.target))
      .map(edge => {
        const source = positions.get(edge.source);
        const target = positions.get(edge.target);
        return `<line x1="${source.x.toFixed(1)}" y1="${source.y.toFixed(1)}" x2="${target.x.toFixed(1)}" y2="${target.y.toFixed(1)}" stroke="rgba(135, 164, 196, 0.18)" stroke-width="1.2" />`;
      })
      .join("");

    const nodeSvg = nodes.map((node, index) => {
      const point = positions.get(node.id);
      const strong = index < 10;
      const fill = strong ? "rgba(78, 225, 177, 0.92)" : "rgba(117, 168, 255, 0.84)";
      const label = strong
        ? `<text x="${(point.x + 8).toFixed(1)}" y="${(point.y - 8).toFixed(1)}" fill="#dbeaff" font-size="11" font-family="Bahnschrift, Trebuchet MS, sans-serif">${escapeHtml(node.id)}</text>`
        : "";
      return `
        <g>
          <circle cx="${point.x.toFixed(1)}" cy="${point.y.toFixed(1)}" r="${point.r.toFixed(1)}" fill="${fill}" fill-opacity="${strong ? "1" : "0.82"}" />
          ${label}
        </g>
      `;
    }).join("");

    refs.networkStage.innerHTML = `
      <div class="network-shell">
        <div class="network-legend">
          <span class="gf-node">Higher-degree sampled nodes</span>
          <span class="gx-node">Other sampled nodes</span>
        </div>
        <svg class="network-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Sample graph preview">
          ${edgeSvg}
          ${nodeSvg}
        </svg>
      </div>
    `;
    renderNetwork3d(preview);
    refs.networkNote.textContent = `${preview.dataset_name}: showing ${nodes.length} sampled nodes and ${edges.length} sampled edges from the selected dataset.`;
  } catch (error) {
    state.currentNetworkPreview = null;
    refs.networkStage.innerHTML = emptyStateHtml(error.message);
    refs.networkStage3d.innerHTML = emptyStateHtml(error.message);
    refs.networkNote.textContent = "The network preview could not be generated.";
  }
}

function renderNetwork3d(preview) {
  const width = refs.networkStage3d.clientWidth || 980;
  const height = 420;
  const nodes = (preview.nodes || []).slice(0, 70);
  const edges = (preview.edges || []).slice(0, 180);
  const maxDegree = Math.max(1, ...nodes.map(node => Number(node.degree || 1)));

  const points = nodes.map((node, index) => {
    const phi = Math.acos(1 - (2 * (index + 0.5)) / nodes.length);
    const theta = Math.PI * (1 + Math.sqrt(5)) * (index + 0.5);
    const radius = 120 + (Number(node.degree || 0) / maxDegree) * 42;
    return {
      id: node.id,
      degree: Number(node.degree || 0),
      x: Math.cos(theta) * Math.sin(phi) * radius,
      y: Math.sin(theta) * Math.sin(phi) * radius,
      z: Math.cos(phi) * radius,
    };
  });

  const pointMap = new Map(points.map(point => [point.id, point]));

  refs.networkStage3d.innerHTML = `
    <div class="network-canvas-wrap">
      <div class="canvas-badge">Auto-rotating 3D sample graph</div>
      <canvas id="graph-canvas-3d" class="network-canvas" width="${width}" height="${height}"></canvas>
    </div>
  `;

  const canvas = document.getElementById("graph-canvas-3d");
  const ctx = canvas.getContext("2d");
  let angle = 0;

  const project = point => {
    const cos = Math.cos(angle);
    const sin = Math.sin(angle);
    const x1 = point.x * cos - point.z * sin;
    const z1 = point.x * sin + point.z * cos;
    const cosY = Math.cos(angle * 0.6);
    const sinY = Math.sin(angle * 0.6);
    const y1 = point.y * cosY - z1 * sinY;
    const z2 = point.y * sinY + z1 * cosY;
    const scale = 420 / (420 + z2 + 220);
    return {
      sx: width / 2 + x1 * scale,
      sy: height / 2 + y1 * scale,
      scale,
      depth: z2,
    };
  };

  const draw = () => {
    ctx.clearRect(0, 0, width, height);

    const projected = new Map(points.map(point => [point.id, { point, screen: project(point) }]));

    edges.forEach(edge => {
      const source = projected.get(edge.source);
      const target = projected.get(edge.target);
      if (!source || !target) {
        return;
      }
      const alpha = Math.max(0.08, (source.screen.scale + target.screen.scale) / 2 * 0.45);
      ctx.strokeStyle = `rgba(135, 164, 196, ${alpha})`;
      ctx.lineWidth = Math.max(0.6, (source.screen.scale + target.screen.scale) * 1.6);
      ctx.beginPath();
      ctx.moveTo(source.screen.sx, source.screen.sy);
      ctx.lineTo(target.screen.sx, target.screen.sy);
      ctx.stroke();
    });

    [...projected.values()]
      .sort((a, b) => a.screen.depth - b.screen.depth)
      .forEach(({ point, screen }, index) => {
        const strong = point.degree >= maxDegree * 0.5 || index >= projected.size - 10;
        const radius = Math.max(2.6, screen.scale * (3.6 + (point.degree / maxDegree) * 9));
        ctx.beginPath();
        ctx.fillStyle = strong ? "rgba(78, 225, 177, 0.95)" : "rgba(117, 168, 255, 0.82)";
        ctx.arc(screen.sx, screen.sy, radius, 0, Math.PI * 2);
        ctx.fill();
        if (strong && screen.scale > 0.62) {
          ctx.fillStyle = "rgba(227, 240, 255, 0.88)";
          ctx.font = "11px Bahnschrift, Trebuchet MS, sans-serif";
          ctx.fillText(point.id, screen.sx + radius + 3, screen.sy - radius);
        }
      });

    angle += 0.008;
    if (state.graphView === "3d") {
      state.orbitFrame = requestAnimationFrame(draw);
    }
  };

  if (state.graphView === "3d") {
    draw();
  } else {
    state.orbitFrame = requestAnimationFrame(() => {
      if (state.graphView === "3d") {
        draw();
      }
    });
  }
}

function renderPagerank(payload) {
  const pagerank = payload.pagerank || {};
  const tableHtml = (title, rows) => `
    <section class="mini-table">
      <table>
        <thead>
          <tr><th colspan="2">${escapeHtml(title)}</th></tr>
          <tr><th>Node</th><th>Score</th></tr>
        </thead>
        <tbody>
          ${(rows || []).map(row => `<tr><td>${escapeHtml(row.node)}</td><td>${escapeHtml(row.score)}</td></tr>`).join("")}
        </tbody>
      </table>
    </section>
  `;

  refs.pagerankGrid.innerHTML = [
    tableHtml("GraphFrames", pagerank.graphframes),
    tableHtml("GraphX", pagerank.graphx),
  ].join("");

  const overlap = pagerank.overlap || { count: 0, total: 0, nodes: [] };
  refs.pagerankOverlap.textContent = `Top-node overlap: ${overlap.count} / ${overlap.total}. Shared nodes: ${(overlap.nodes || []).join(", ") || "None"}.`;
}

function renderRunCards(payload) {
  const runs = payload.runs || [];
  refs.runCards.innerHTML = runs.map(run => `
    <article class="run-card">
      <div class="run-head">
        <h3>${escapeHtml(run.name)}</h3>
        <span class="pill ${run.success ? "ok" : "bad"}">${run.success ? "Success" : "Needs attention"}</span>
      </div>
      <div class="run-meta">
        <span>Runtime</span><strong>${escapeHtml(run.runtime)}</strong>
        <span>Return code</span><strong>${escapeHtml(run.returncode)}</strong>
        <span>Command</span><strong>${escapeHtml(run.command)}</strong>
        <span>Log file</span><strong>${escapeHtml(run.log_file)}</strong>
      </div>
    </article>
  `).join("");
}

function renderResearch(payload) {
  const facts = payload.research_facts || [];
  refs.researchGrid.innerHTML = facts.map(fact => `
    <article class="research-card">
      <div class="research-head">
        <h3>${escapeHtml(fact.aspect)}</h3>
        <span class="pill neutral">Source-backed</span>
      </div>
      <div class="research-copy">
        <div><strong>GraphFrames:</strong> <span class="fact-copy">${escapeHtml(fact.graphframes)}</span></div>
        <div><strong>GraphX:</strong> <span class="fact-copy">${escapeHtml(fact.graphx)}</span></div>
        <div><strong>Project angle:</strong> <span class="fact-copy">${escapeHtml(fact.social_media)}</span></div>
      </div>
      <a href="${escapeHtml(fact.source_url)}" target="_blank" rel="noreferrer">${escapeHtml(fact.source_label)}</a>
    </article>
  `).join("");
}

function renderLogs(payload) {
  refs.gfLogPath.textContent = payload.log_paths?.graphframes || "";
  refs.gxLogPath.textContent = payload.log_paths?.graphx || "";
  refs.gfLog.textContent = (payload.log_snippets?.graphframes || []).join("\n");
  refs.gxLog.textContent = (payload.log_snippets?.graphx || []).join("\n");
}

function syncJobUi(job) {
  if (!job) {
    refs.jobPill.className = "pill neutral";
    refs.jobPill.textContent = "Ready";
    refs.jobMessage.textContent = "Choose a dataset to load saved proof or start a new run.";
    refs.jobProgress.style.width = "0%";
    refs.runButton.disabled = false;
    refs.runButton.textContent = "Run Selected Dataset";
    return;
  }

  const className = statusClass(job.status);
  refs.jobPill.className = `pill ${className}`;
  refs.jobPill.textContent = String(job.status).toUpperCase();
  refs.jobMessage.textContent = job.message || "";
  refs.jobProgress.style.width = `${Number(job.progress || 0)}%`;
  const active = job.status === "queued" || job.status === "running";
  refs.runButton.disabled = active;
  refs.runButton.textContent = active ? "Running..." : "Run Selected Dataset";
}

async function startPolling(jobId) {
  stopPolling();
  const poll = async () => {
    const job = await apiGet(`/api/jobs/${encodeURIComponent(jobId)}`);
    state.activeJob = job;
    syncJobUi(job);
    if (job.status === "completed") {
      stopPolling();
      await refreshDatasets();
      await loadPayload(job.result_slug);
      syncJobUi(null);
    } else if (job.status === "failed") {
      stopPolling();
      refs.jobMessage.textContent = job.traceback || job.error || job.message || "Run failed.";
      refs.jobPill.className = "pill bad";
      refs.jobPill.textContent = "FAILED";
      refs.runButton.disabled = false;
      refs.runButton.textContent = "Run Selected Dataset";
    }
  };

  await poll();
  state.pollTimer = setInterval(() => {
    poll().catch(error => {
      stopPolling();
      refs.jobPill.className = "pill bad";
      refs.jobPill.textContent = "ERROR";
      refs.jobMessage.textContent = error.message;
    });
  }, 2500);
}

function stopPolling() {
  if (state.pollTimer) {
    clearInterval(state.pollTimer);
    state.pollTimer = null;
  }
}

function handleProofClick(event) {
  const button = event.target.closest("button[data-proof-index]");
  if (!button || !state.payload) {
    return;
  }
  const metric = state.payload.metrics?.[Number(button.dataset.proofIndex)];
  if (!metric) {
    return;
  }
  openProof(metric);
}

async function getPreview(path) {
  if (!path) {
    return { path: "", content: "No preview path available." };
  }
  if (state.previewCache.has(path)) {
    return state.previewCache.get(path);
  }
  const preview = await apiGet(`/api/proof-preview?path=${encodeURIComponent(path)}`);
  state.previewCache.set(path, preview);
  return preview;
}

async function openProof(metric) {
  const proof = metric.proof;
  refs.modal.classList.remove("hidden");
  refs.proofTitle.textContent = metric.label;
  refs.proofNote.textContent = proof.note;
  refs.proofSummary.innerHTML = [
    cardHtml("Status", proof.status, "How this row should be interpreted."),
    cardHtml("GraphFrames", proof.values.graphframes, "Value exported for this dataset."),
    cardHtml("GraphX", proof.values.graphx, "Value exported for this dataset."),
  ].join("");
  refs.proofGfPath.textContent = proof.files.graphframes || "";
  refs.proofGxPath.textContent = proof.files.graphx || "";
  refs.proofGfPreview.textContent = "Loading preview...";
  refs.proofGxPreview.textContent = "Loading preview...";

  try {
    const [gfPreview, gxPreview] = await Promise.all([
      getPreview(proof.files.graphframes),
      getPreview(proof.files.graphx),
    ]);
    refs.proofGfPreview.textContent = gfPreview.content || "No content available.";
    refs.proofGxPreview.textContent = gxPreview.content || "No content available.";
  } catch (error) {
    refs.proofGfPreview.textContent = error.message;
    refs.proofGxPreview.textContent = error.message;
  }
}

function closeModal() {
  refs.modal.classList.add("hidden");
}

bootstrap().catch(error => {
  refs.jobPill.className = "pill bad";
  refs.jobPill.textContent = "ERROR";
  refs.jobMessage.textContent = error.message;
  renderEmptyDashboard("The application could not load its initial data.");
});
