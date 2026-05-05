/**
 * PAINEL LEADS LITE - CORE ENGINE (FUSÃO FINAL)
 * Integrado com BigQuery API, TomSelect e Dracula UI.
 */

const $ = (sel) => document.querySelector(sel);

// Instâncias Globais do TomSelect
let tsStatus, tsCurso, tsModalidade, tsTurno, tsPolo, tsOrigem;
let tsConsultorDisparo, tsConsultorComercial, tsCanal, tsCampanha;
let tsTipoDisparo, tsTipoNegocio;

// Configurações e Estado
const TABLE_COLS = 13;
const EMPTY_FILTER_TOKEN = "__EMPTY__";
const EMPTY_FILTER_LABEL = "(Sem preenchimento)";
const SAVED_FILTERS_STORAGE_KEY = "painel_leads_saved_filters_v1";
const MAX_SAVED_FILTERS = 5;

let currentPage = 1;
let totalLeads = 0;
let isLoadingLeads = false;
let activeExportJobId = null;

/* ============================================================
   HELPERS DE UI E FORMATAÇÃO
   ============================================================ */
function setStatus(msg, type = "ok") {
    const el = $("#statusLine") || { textContent: "" };
    el.textContent = msg;
    el.className = `status-line ${type === "err" ? "error" : "status-ok"}`;
    
    // Feedback visual no botão de aplicação se estiver carregando
    const btnApply = $("#btnApply");
    if (btnApply) {
        if (msg.includes("Consultando")) {
            btnApply.classList.add("loading");
            btnApply.textContent = "Buscando...";
        } else {
            btnApply.classList.remove("loading");
            btnApply.textContent = "Aplicar Filtros";
        }
    }
}

function escapeHtml(str) {
    if (str === null || str === undefined) return "";
    return String(str).replace(/[&<>"']/g, (m) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    }[m]));
}

function fmtDate(d) {
    if (!d || d === "None") return "-";
    const s = String(d).slice(0, 10);
    return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s.split("-").reverse().join("/") : s;
}

function fmtBool(b) {
    if (b === true || String(b).toLowerCase() === "true") return "Sim";
    if (b === false || String(b).toLowerCase() === "false") return "Não";
    return "-";
}

/* ============================================================
   COMUNICAÇÃO COM API (BIGQUERY / FLASK)
   ============================================================ */
async function apiRequest(path, method = "GET", body = null) {
    const options = {
        method,
        headers: body ? { "Content-Type": "application/json" } : {},
        body: body ? JSON.stringify(body) : null,
        credentials: "same-origin"
    };

    const res = await fetch(path, options);
    const text = await res.text();
    let data;
    try { data = text ? JSON.parse(text) : {}; } catch { data = { message: text }; }

    if (!res.ok) {
        if (res.status === 401 && data.redirect_to) {
            window.location.href = data.redirect_to;
            throw new Error("Sessão expirada");
        }
        throw new Error(data.error || data.message || `Erro ${res.status}`);
    }
    return data;
}

/* ============================================================
   GESTÃO DO TOMSELECT
   ============================================================ */
function makeTomSelect(selector) {
    const el = $(selector);
    if (!el) return null;

    const ts = new TomSelect(selector, {
        plugins: ['checkbox_options', 'remove_button'],
        maxItems: null,
        hideSelected: false,
        closeAfterSelect: false,
        valueField: "value",
        labelField: "text",
        searchField: ["text"],
        render: {
            option: (data, escape) => `
                <div class="ts-opt">
                    <span class="ts-opt-check"></span>
                    <span class="ts-opt-text">${escape(data.text)}</span>
                </div>`,
            item: (data, escape) => `<div class="ts-particle">${escape(data.text)}</div>`
        },
        onChange: () => {
            currentPage = 1;
            loadLeadsAndKpisDebounced();
        }
    });
    return ts;
}

function fillSelect(ts, values) {
    if (!ts) return;
    ts.clearOptions();
    ts.addOption({ value: EMPTY_FILTER_TOKEN, text: EMPTY_FILTER_LABEL });
    (values || []).forEach(v => v && ts.addOption({ value: String(v), text: String(v) }));
    ts.refreshOptions(false);
}

/* ============================================================
   LÓGICA DE DADOS (SEARCH & KPIS)
   ============================================================ */
function buildParams() {
    const getV = (ts) => ts ? ts.getValue() : [];
    return {
        status: getV(tsStatus),
        curso: getV(tsCurso),
        modalidade: getV(tsModalidade),
        turno: getV(tsTurno),
        polo: getV(tsPolo),
        origem: getV(tsOrigem),
        consultor_disparo: getV(tsConsultorDisparo),
        consultor_comercial: getV(tsConsultorComercial),
        canal: getV(tsCanal),
        campanha: getV(tsCampanha),
        tipo_disparo: getV(tsTipoDisparo),
        tipo_negocio: getV(tsTipoNegocio),
        data_ini: $("#fIni")?.value || "",
        data_fim: $("#fFim")?.value || "",
        matriculado: $("#fMatriculado")?.value || "",
        limit: Number($("#fLimit")?.value) || 500,
        offset: (currentPage - 1) * (Number($("#fLimit")?.value) || 500),
        busca_txt: $("#fBusca")?.value || ""
    };
}

async function loadLeadsAndKpis() {
    if (isLoadingLeads) return;
    isLoadingLeads = true;
    setStatus("Consultando BigQuery...", "ok");

    const params = buildParams();
    try {
        const [leadsResp, kpisResp] = await Promise.all([
            apiRequest("/api/leads/search", "POST", params),
            apiRequest("/api/kpis/search", "POST", params)
        ]);

        renderTable(leadsResp.data || []);
        renderPagination(leadsResp.total || 0, (leadsResp.data || []).length);
        renderKpis(kpisResp);
        setStatus(`${(leadsResp.data || []).length} registros carregados.`, "ok");
    } catch (e) {
        setStatus(e.message, "err");
        renderTable([]);
    } finally {
        isLoadingLeads = false;
    }
}

const loadLeadsAndKpisDebounced = (() => {
    let t; return () => { clearTimeout(t); t = setTimeout(loadLeadsAndKpis, 500); };
})();

/* ============================================================
   RENDERIZAÇÃO
   ============================================================ */
function renderTable(rows) {
    const tbody = $("#tbl tbody");
    if (!tbody) return;
    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="${TABLE_COLS}" class="table-feedback">Nenhum dado encontrado</td></tr>`;
        return;
    }
    tbody.innerHTML = rows.map(r => `
        <tr>
            <td>${escapeHtml(fmtDate(r.data_inscricao))}</td>
            <td>${escapeHtml(r.nome || "-")}</td>
            <td>${escapeHtml(r.cpf || "-")}</td>
            <td>${escapeHtml(r.celular || "-")}</td>
            <td>${escapeHtml(r.origem || "-")}</td>
            <td>${escapeHtml(r.polo || "-")}</td>
            <td>${escapeHtml(r.curso || "-")}</td>
            <td>${escapeHtml(r.modalidade || "-")}</td>
            <td><span class="badge">${escapeHtml(r.status_inscricao || "LEAD")}</span></td>
            <td>${fmtBool(r.flag_matriculado)}</td>
            <td>${escapeHtml(r.consultor_disparo || "-")}</td>
            <td>${escapeHtml(r.campanha || "-")}</td>
            <td>${escapeHtml(r.canal || "-")}</td>
        </tr>`).join("");
}

function renderPagination(total, shown) {
    totalLeads = total;
    const limit = Number($("#fLimit")?.value) || 500;
    const start = total > 0 ? ((currentPage - 1) * limit) + 1 : 0;
    const end = start + shown - 1;

    if ($("#lblRange")) $("#lblRange").textContent = `Mostrando ${start}-${Math.max(end, 0)} de ${total} leads`;
    if ($("#lblPage")) $("#lblPage").textContent = `Página ${currentPage}`;
    if ($("#btnPrevPage")) $("#btnPrevPage").disabled = currentPage <= 1;
    if ($("#btnNextPage")) $("#btnNextPage").disabled = end >= total;
    if ($("#kpiCount")) $("#kpiCount").textContent = total;
}

function renderKpis(k) {
    const top = k?.top_status;
    if ($("#kpiTopStatus")) $("#kpiTopStatus").textContent = top ? `${top.status} (${top.cnt})` : "-";
}

/* ============================================================
   EXPORTAÇÃO E INTERFACE DE DROPDOWN
   ============================================================ */
function initExportUI() {
    const btnToggle = $("#btnExportToggle");
    const dropdown = $("#exportDropdown");

    if (btnToggle && dropdown) {
        btnToggle.addEventListener('click', (e) => {
            e.stopPropagation();
            dropdown.classList.toggle('active');
        });
        document.addEventListener('click', () => dropdown.classList.remove('active'));
    }

    $("#btnExport")?.addEventListener("click", () => {
        const params = buildParams();
        const url = new URL("/api/export/xlsx", window.location.origin);
        Object.entries(params).forEach(([k, v]) => {
            if (Array.isArray(v)) { if (v.length) url.searchParams.set(k, v.join(" || ")); }
            else if (v) url.searchParams.set(k, v);
        });
        window.location.href = url.toString();
    });

    $("#btnBatchExport")?.addEventListener("click", async () => {
        if (!confirm("Deseja iniciar exportação em lote para grandes volumes?")) return;
        try {
            const resp = await apiRequest("/api/export/batch", "POST", buildParams());
            alert("Processamento em lote iniciado. Job ID: " + resp.job_id);
        } catch (e) { alert("Erro: " + e.message); }
    });
}

/* ============================================================
   INICIALIZAÇÃO
   ============================================================ */
document.addEventListener("DOMContentLoaded", async () => {
    // 1. Iniciar TomSelects
    tsStatus = makeTomSelect("#fStatus");
    tsCurso = makeTomSelect("#fCurso");
    tsModalidade = makeTomSelect("#fModalidade");
    tsTurno = makeTomSelect("#fTurno");
    tsPolo = makeTomSelect("#fPolo");
    tsOrigem = makeTomSelect("#fOrigem");
    tsConsultorDisparo = makeTomSelect("#fConsultorDisparo");
    tsConsultorComercial = makeTomSelect("#fConsultorComercial");
    tsCanal = makeTomSelect("#fCanal");
    tsCampanha = makeTomSelect("#fCampanha");
    tsTipoDisparo = makeTomSelect("#fTipoDisparo");
    tsTipoNegocio = makeTomSelect("#fTipoNegocio");

    // 2. Iniciar UI de Exportação
    initExportUI();

    // 3. Eventos de Botões
    $("#btnApply")?.addEventListener("click", () => { currentPage = 1; loadLeadsAndKpis(); });
    $("#btnReload")?.addEventListener("click", () => window.location.reload());
    $("#btnClear")?.addEventListener("click", () => {
        [tsStatus, tsCurso, tsModalidade, tsTurno, tsPolo, tsOrigem, tsConsultorDisparo, tsConsultorComercial, tsCanal, tsCampanha, tsTipoDisparo, tsTipoNegocio].forEach(ts => ts?.clear());
        $("#fBusca").value = "";
        currentPage = 1;
        loadLeadsAndKpis();
    });

    $("#btnPrevPage")?.addEventListener("click", () => { if (currentPage > 1) { currentPage--; loadLeadsAndKpis(); } });
    $("#btnNextPage")?.addEventListener("click", () => { currentPage++; loadLeadsAndKpis(); });

    // 4. Carga Inicial de Dados
    try {
        const optResp = await apiRequest("/api/options");
        const d = optResp.data || optResp;
        fillSelect(tsStatus, d.status);
        fillSelect(tsCurso, d.cursos);
        fillSelect(tsModalidade, d.modalidades);
        fillSelect(tsTurno, d.turnos);
        fillSelect(tsPolo, d.polos);
        fillSelect(tsOrigem, d.origens);
        fillSelect(tsConsultorDisparo, d.consultores_disparo);
        fillSelect(tsConsultorComercial, d.consultores_comercial);
        fillSelect(tsCanal, d.canais);
        fillSelect(tsCampanha, d.campanhas);
        fillSelect(tsTipoDisparo, d.tipos_disparo);
        fillSelect(tsTipoNegocio, d.tipos_negocio);
    } catch (e) { console.error("Erro options:", e); }

    loadLeadsAndKpis();
});
