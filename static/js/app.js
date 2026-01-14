/* ============================================================
   Painel Leads Lite — app.js
   Chips + Dropdown custom (overlay) + infinite scroll + export server
============================================================ */

const $ = (sel) => document.querySelector(sel);

function showToast(msg, type = "ok") {
  const statusLine = $("#statusLine");
  if (statusLine) {
    statusLine.textContent = msg;
    statusLine.className = type === "err" ? "error" : "";
  } else {
    alert(msg);
  }
}

function setUploadStatus(msg, type = "muted") {
  const el = $("#uploadStatus");
  if (!el) return;
  el.textContent = msg || "";
  el.className = type;
}

function escapeHtml(str) {
  if (str === null || str === undefined) return "";
  return String(str)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function fmtDate(d) {
  if (!d) return "";
  return String(d).slice(0, 10);
}

function debounce(fn, delay = 300) {
  let t = null;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), delay);
  };
}

/* ============================================================
   State
============================================================ */
const state = {
  options: { status: [], curso: [], polo: [], origem: [] },
  filters: {
    status: "",
    origem: "",
    data_ini: "",
    data_fim: "",
    limit: 500,
    curso_list: [],
    polo_list: [],
  },

  // dropdown runtime
  dd: {
    curso: { q: "", idx: 0, filtered: [], open: false },
    polo: { q: "", idx: 0, filtered: [], open: false },
  },
};

/* ============================================================
   API
============================================================ */
async function apiGet(path, params = {}) {
  const url = new URL(path, window.location.origin);

  Object.entries(params).forEach(([k, v]) => {
    if (v === null || v === undefined) return;

    if (Array.isArray(v)) {
      v.forEach((item) => {
        const s = String(item ?? "").trim();
        if (s) url.searchParams.append(k, s);
      });
      return;
    }

    const s = String(v).trim();
    if (s) url.searchParams.set(k, s);
  });

  const res = await fetch(url.toString(), { cache: "no-store" });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function apiPostForm(path, formData) {
  const res = await fetch(path, { method: "POST", body: formData });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.error || `HTTP ${res.status}`);
  return data;
}

/* ============================================================
   UI: table + KPIs
============================================================ */
function renderTable(rows) {
  const tbody = $("#tbl tbody");
  if (!tbody) return;

  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="10" class="muted">Nenhum lead encontrado.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows.map((r) => `
    <tr>
      <td>${escapeHtml(fmtDate(r.data_inscricao))}</td>
      <td>${escapeHtml(r.nome)}</td>
      <td>${escapeHtml(r.cpf)}</td>
      <td>${escapeHtml(r.celular)}</td>
      <td>${escapeHtml(r.email)}</td>
      <td>${escapeHtml(r.origem)}</td>
      <td>${escapeHtml(r.polo)}</td>
      <td>${escapeHtml(r.curso)}</td>
      <td>${escapeHtml(r.status)}</td>
      <td>${escapeHtml(r.consultor)}</td>
    </tr>
  `).join("");
}

function renderKpis(k) {
  const total = $("#kpiCount");
  const top = $("#kpiTopStatus");
  const last = $("#kpiLastDate");

  if (total) total.textContent = k?.total ?? 0;

  if (top) {
    const s = k?.top_status?.status ?? "-";
    const c = k?.top_status?.cnt ?? 0;
    top.textContent = s === "-" ? "-" : `${s} (${c})`;
  }

  if (last) last.textContent = k?.last_date ? fmtDate(k.last_date) : "-";
}

function fillDatalist(id, values) {
  const dl = document.getElementById(id);
  if (!dl) return;
  dl.innerHTML = "";
  (values || []).forEach((v) => {
    const opt = document.createElement("option");
    opt.value = v;
    dl.appendChild(opt);
  });
}

/* ============================================================
   Chips
============================================================ */
function _uniqPush(arr, value) {
  const v = String(value || "").trim();
  if (!v) return;
  if (!arr.some((x) => String(x).toUpperCase() === v.toUpperCase())) arr.push(v);
}

function _removeValue(arr, value) {
  const v = String(value || "").trim().toUpperCase();
  const idx = arr.findIndex((x) => String(x).trim().toUpperCase() === v);
  if (idx >= 0) arr.splice(idx, 1);
}

function renderChips(kind) {
  const box = kind === "curso" ? $("#cursoChipBox") : $("#poloChipBox");
  const hidden = kind === "curso" ? $("#fCursoMulti") : $("#fPoloMulti");
  const list = kind === "curso" ? state.filters.curso_list : state.filters.polo_list;

  if (!box || !hidden) return;

  hidden.value = list.join("||");
  box.innerHTML = "";

  if (!list.length) {
    const empty = document.createElement("div");
    empty.className = "muted";
    empty.textContent = "Nenhum selecionado.";
    box.appendChild(empty);
    return;
  }

  list.forEach((v) => {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "chip";
    chip.innerHTML = `<span class="chip-text">${escapeHtml(v)}</span><span class="chip-x">×</span>`;
    chip.addEventListener("click", () => {
      _removeValue(list, v);
      renderChips(kind);
      ddRebuild(kind); // atualiza tags "Selecionado"
    });
    box.appendChild(chip);
  });
}

/* ============================================================
   Dropdown custom — overlay + busca + infinite scroll
   Requisitos no HTML:
   - input #fCurso e dropdown div #ddCursos
   - input #fPolo e dropdown div #ddPolos
============================================================ */
function ddGet(kind) {
  return kind === "curso" ? $("#ddCursos") : $("#ddPolos");
}
function ddInput(kind) {
  return kind === "curso" ? $("#fCurso") : $("#fPolo");
}
function ddAll(kind) {
  return kind === "curso" ? (state.options.curso || []) : (state.options.polo || []);
}
function ddSelected(kind) {
  return kind === "curso" ? state.filters.curso_list : state.filters.polo_list;
}
function ddState(kind) {
  return kind === "curso" ? state.dd.curso : state.dd.polo;
}

function ddOpen(kind) {
  const dd = ddGet(kind);
  if (!dd) return;
  dd.hidden = false;
  ddState(kind).open = true;
}
function ddClose(kind) {
  const dd = ddGet(kind);
  if (!dd) return;
  dd.hidden = true;
  ddState(kind).open = false;
}

function ddMakeRow(kind, v) {
  const selected = ddSelected(kind);
  const isOn = selected.some((x) => String(x).trim().toUpperCase() === String(v).trim().toUpperCase());

  const row = document.createElement("div");
  row.className = "dd-item" + (isOn ? " dd-on" : "");
  row.innerHTML = `
    <div class="dd-text">${escapeHtml(v)}</div>
    <div class="dd-tag">${isOn ? "Selecionado" : "Adicionar"}</div>
  `;

  row.addEventListener("mousedown", (e) => {
    e.preventDefault(); // evita blur antes do click
    if (isOn) _removeValue(selected, v);
    else _uniqPush(selected, v);

    renderChips(kind);
    ddRebuild(kind);       // reflete "Selecionado"
    loadLeadsAndKpisDebounced();
  });

  return row;
}

function ddFilterList(kind, qUpper) {
  const all = ddAll(kind);
  if (!qUpper) return all.slice(); // copia

  // filtra sem travar: percorre tudo, mas só monta array simples
  const out = [];
  for (let i = 0; i < all.length; i++) {
    const v = String(all[i] || "");
    if (!v) continue;
    if (v.toUpperCase().includes(qUpper)) out.push(v);
  }
  return out;
}

// carrega por lote (infinite scroll)
const DD_BATCH = 350;

function ddRenderNext(kind) {
  const dd = ddGet(kind);
  const st = ddState(kind);
  if (!dd) return;

  // se vazio, mostra empty
  if (!st.filtered || st.filtered.length === 0) {
    dd.innerHTML = `<div class="dd-empty muted">Nada encontrado.</div>`;
    st.idx = 0;
    return;
  }

  // primeira renderização limpa e cria “top spacer” opcional (não precisa)
  if (st.idx === 0) dd.innerHTML = "";

  const end = Math.min(st.idx + DD_BATCH, st.filtered.length);

  // fragmento pra performance
  const frag = document.createDocumentFragment();
  for (let i = st.idx; i < end; i++) {
    frag.appendChild(ddMakeRow(kind, st.filtered[i]));
  }
  dd.appendChild(frag);

  st.idx = end;

  // “rodapé” indicando carregamento/total
  const footerId = `ddFooter_${kind}`;
  let footer = dd.querySelector(`#${footerId}`);
  if (!footer) {
    footer = document.createElement("div");
    footer.id = footerId;
    footer.className = "dd-footer muted";
    dd.appendChild(footer);
  }

  if (st.idx >= st.filtered.length) {
    footer.textContent = `Fim — ${st.filtered.length} itens`;
  } else {
    footer.textContent = `Mostrando ${st.idx} de ${st.filtered.length} — role para carregar mais`;
  }
}

function ddRebuild(kind) {
  const dd = ddGet(kind);
  const input = ddInput(kind);
  if (!dd || !input) return;

  const st = ddState(kind);
  const q = (input.value || "").trim().toUpperCase();
  st.q = q;
  st.filtered = ddFilterList(kind, q);
  st.idx = 0;

  ddRenderNext(kind);
}

function ddAttachScroll(kind) {
  const dd = ddGet(kind);
  if (!dd) return;

  dd.addEventListener("scroll", () => {
    const st = ddState(kind);
    if (!st.open) return;

    // quando estiver perto do fim, renderiza próximo batch
    const nearBottom = dd.scrollTop + dd.clientHeight >= dd.scrollHeight - 60;
    if (nearBottom && st.idx < st.filtered.length) {
      ddRenderNext(kind);
    }
  });
}

function bindDropdown(kind) {
  const input = ddInput(kind);
  const dd = ddGet(kind);
  if (!input || !dd) return;

  // posicionamento: garante que o dropdown aparece “grudado” no input (overlay)
  // (CSS faz o resto)

  input.addEventListener("focus", () => {
    ddOpen(kind);
    ddRebuild(kind); // abre e renderiza batch inicial
  });

  input.addEventListener("input", debounce(() => {
    ddOpen(kind);
    ddRebuild(kind); // refaz filtro e reinicia batches
  }, 120));

  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      const v = (input.value || "").trim();
      if (v) {
        _uniqPush(ddSelected(kind), v);
        input.value = "";
        renderChips(kind);
        ddRebuild(kind);
        loadLeadsAndKpisDebounced();
      }
    }
    if (e.key === "Escape") {
      ddClose(kind);
    }
  });

  // fecha clicando fora
  document.addEventListener("mousedown", (e) => {
    if (e.target === input) return;
    if (dd.contains(e.target)) return;
    ddClose(kind);
  });

  ddAttachScroll(kind);
}

/* ============================================================
   Filters
============================================================ */
function readFiltersFromUI() {
  state.filters.status = ($("#fStatus")?.value || "").trim();
  state.filters.origem = ($("#fOrigem")?.value || "").trim();
  state.filters.data_ini = ($("#fIni")?.value || "").trim();
  state.filters.data_fim = ($("#fFim")?.value || "").trim();

  const lim = parseInt($("#fLimit")?.value || "500", 10);
  state.filters.limit = Number.isFinite(lim) ? lim : 500;
}

/* ============================================================
   Loaders
============================================================ */
async function loadOptions() {
  const data = await apiGet("/api/options");
  state.options = {
    status: data.status || [],
    curso: data.curso || [],
    polo: data.polo || [],
    origem: data.origem || [],
  };

  // esses ainda usam datalist normal
  fillDatalist("dlStatus", state.options.status);
  fillDatalist("dlOrigem", state.options.origem);

  renderChips("curso");
  renderChips("polo");

  // se dropdown estiver aberto, refaz
  if (ddState("curso").open) ddRebuild("curso");
  if (ddState("polo").open) ddRebuild("polo");
}

async function loadLeadsAndKpis() {
  readFiltersFromUI();

  const statusLine = $("#statusLine");
  if (statusLine) statusLine.textContent = "Carregando...";

  const params = {
    status: state.filters.status,
    origem: state.filters.origem,
    data_ini: state.filters.data_ini,
    data_fim: state.filters.data_fim,
    limit: state.filters.limit,

    // multi
    curso: state.filters.curso_list,
    polo: state.filters.polo_list,
  };

  try {
    const [leads, kpis] = await Promise.all([
      apiGet("/api/leads", params),
      apiGet("/api/kpis", params),
    ]);

    const rows = leads?.rows || [];
    renderTable(rows);
    renderKpis(kpis);

    const count = leads?.count ?? rows.length ?? 0;
    if (statusLine) statusLine.textContent = `${count} registros carregados.`;
  } catch (e) {
    console.error(e);
    renderTable([]);
    showToast("Falha ao carregar dados do BigQuery.", "err");
  }
}

const loadLeadsAndKpisDebounced = debounce(loadLeadsAndKpis, 250);

/* ============================================================
   Upload
============================================================ */
function bindUpload() {
  const file = $("#uploadFile");
  const source = $("#uploadSource");
  const btn = $("#btnUpload");
  if (!file || !btn) return;

  btn.addEventListener("click", async (e) => {
    e.preventDefault();

    if (!file.files || file.files.length === 0) {
      setUploadStatus("Selecione um arquivo CSV ou XLSX.", "warn");
      return;
    }

    const f = file.files[0];
    const fd = new FormData();
    fd.append("file", f);
    if (source && source.value) fd.append("source", source.value);

    btn.disabled = true;
    setUploadStatus("Enviando...", "muted");

    try {
      const data = await apiPostForm("/api/upload", fd);
      const rows = data.rows_loaded ?? 0;
      const fname = data.filename ?? f.name ?? "arquivo";
      const msg = data.message ?? "Upload concluído.";

      setUploadStatus(`${msg} (${rows} linhas) — ${fname}`, "ok");
      showToast("Upload finalizado. Atualizando dados...", "ok");

      await loadOptions();
      await loadLeadsAndKpis();
      showToast("Painel atualizado com os novos dados.", "ok");
    } catch (err) {
      console.error(err);
      setUploadStatus(`Falha: ${err.message}`, "error");
      showToast(`Falha no upload: ${err.message}`, "err");
    } finally {
      btn.disabled = false;
      file.value = "";
    }
  });
}

/* ============================================================
   Reload + Export (server)
============================================================ */
function bindReload() {
  const btn = $("#btnReload");
  if (!btn) return;
  btn.addEventListener("click", async (e) => {
    e.preventDefault();
    await loadOptions();
    await loadLeadsAndKpis();
  });
}

function exportCsvServer() {
  readFiltersFromUI();

  const url = new URL("/api/export", window.location.origin);

  if (state.filters.status) url.searchParams.set("status", state.filters.status);
  if (state.filters.origem) url.searchParams.set("origem", state.filters.origem);
  if (state.filters.data_ini) url.searchParams.set("data_ini", state.filters.data_ini);
  if (state.filters.data_fim) url.searchParams.set("data_fim", state.filters.data_fim);

  (state.filters.curso_list || []).forEach((c) => url.searchParams.append("curso", c));
  (state.filters.polo_list || []).forEach((p) => url.searchParams.append("polo", p));

  url.searchParams.set("export_limit", "200000");

  window.open(url.toString(), "_blank", "noopener");
}

function bindExport() {
  const btn = $("#btnExportFilters");
  if (!btn) return;
  btn.addEventListener("click", (e) => {
    e.preventDefault();
    showToast("Exportando CSV...", "ok");
    exportCsvServer();
  });
}

/* ============================================================
   Bind filtros
============================================================ */
function bindFilters() {
  const ids = ["#fStatus", "#fOrigem", "#fIni", "#fFim", "#fLimit"];

  ids.forEach((id) => {
    const el = $(id);
    if (!el) return;
    el.addEventListener("change", loadLeadsAndKpisDebounced);
    el.addEventListener("keyup", loadLeadsAndKpisDebounced);
  });

  $("#btnApply")?.addEventListener("click", (e) => {
    e.preventDefault();
    loadLeadsAndKpis();
  });

  $("#btnClear")?.addEventListener("click", (e) => {
    e.preventDefault();

    ["#fStatus", "#fOrigem", "#fIni", "#fFim"].forEach((id) => {
      const el = $(id);
      if (el) el.value = "";
    });

    const lim = $("#fLimit");
    if (lim) lim.value = "500";

    state.filters.curso_list = [];
    state.filters.polo_list = [];

    const inCurso = $("#fCurso");
    const inPolo = $("#fPolo");
    if (inCurso) inCurso.value = "";
    if (inPolo) inPolo.value = "";

    renderChips("curso");
    renderChips("polo");

    ddClose("curso");
    ddClose("polo");

    setUploadStatus("");
    showToast("Filtros limpos.", "ok");
    loadLeadsAndKpis();
  });
}

/* ============================================================
   Init
============================================================ */
document.addEventListener("DOMContentLoaded", async () => {
  try {
    bindUpload();
    bindFilters();
    bindReload();
    bindExport();

    renderChips("curso");
    renderChips("polo");

    bindDropdown("curso");
    bindDropdown("polo");

    await loadOptions();
    await loadLeadsAndKpis();
  } catch (e) {
    console.error(e);
    showToast("Erro ao iniciar o painel.", "err");
  }
});
