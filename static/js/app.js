/* ============================================================
   Painel Leads Lite — app.js (Versão Final Consolidada)
   - Suporte Total a TomSelect com Checkboxes
   - Integração com BigQuery Backend
============================================================ */

const $ = (sel) => document.querySelector(sel);

// Instâncias globais do TomSelect
let tsCurso, tsPolo;

/* ============================================================
   Helpers & UI
============================================================ */
function showToast(msg, type = "ok") {
    const statusLine = $("#statusLine");
    if (statusLine) {
        statusLine.textContent = msg;
        statusLine.className = type === "err" ? "error" : "";
    }
}

function setUploadStatus(msg, type = "muted") {
    const el = $("#uploadStatus");
    if (!el) return;
    el.textContent = msg || "";
    el.className = type;
}

function escapeHtml(str) {
    if (!str) return "";
    return String(str).replace(/[&<>"']/g, m => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[m]));
}

function fmtDate(d) {
    if (!d || d === "None") return "-";
    const s = String(d);
    if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
    const ts = Date.parse(s);
    return !Number.isNaN(ts) ? new Date(ts).toISOString().slice(0, 10) : s.slice(0, 10);
}

function sortSmart(arr) {
    return (arr || []).slice().sort((a, b) => a.localeCompare(b, 'pt-BR', { sensitivity: 'base' }));
}

/* ============================================================
   API Core
============================================================ */
async function apiGet(path, params = {}) {
    const url = new URL(path, window.location.origin);
    
    Object.entries(params).forEach(([k, v]) => {
        if (v === null || v === undefined || v === "" || (Array.isArray(v) && v.length === 0)) return;
        
        // Se for Array (Curso/Polo), transforma em "VAL1 || VAL2" para o Python
        if (Array.isArray(v)) {
            url.searchParams.set(k, v.join(" || "));
        } else {
            url.searchParams.set(k, v);
        }
    });

    const res = await fetch(url.toString(), { cache: "no-store" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data?.error || `Erro HTTP ${res.status}`);
    return data;
}

/* ============================================================
   Inicialização do TomSelect (Dropdown com Checkbox)
============================================================ */
function initMultiSelects() {
    const config = {
        plugins: ['remove_button', 'checkbox_options'],
        create: false,
        allowEmptyOption: true,
        maxItems: null,
        valueField: 'value',
        labelField: 'text',
        searchField: ['text'],
        // ESTA PARTE GERA O HTML DO CHECKBOX
        render: {
            option: function(data, escape) {
                return `<div class="d-flex"><span class="checkbox"></span><span>${escape(data.text)}</span></div>`;
            },
            item: function(data, escape) {
                return `<div>${escape(data.text)}</div>`;
            }
        },
        onChange: () => loadLeadsAndKpisDebounced()
    };

    if ($("#fCurso")) tsCurso = new TomSelect("#fCurso", config);
    if ($("#fPolo")) tsPolo = new TomSelect("#fPolo", config);
}

/* ============================================================
   Filtros & Carga de Dados
============================================================ */
function getFilters() {
    return {
        status: $("#fStatus")?.value || "",
        curso: tsCurso ? tsCurso.getValue() : [], 
        polo: tsPolo ? tsPolo.getValue() : [],   
        origem: $("#fOrigem")?.value || "",
        data_ini: $("#fIni")?.value || "",
        data_fim: $("#fFim")?.value || "",
        limit: $("#fLimit")?.value || 500
    };
}

async function loadOptions() {
    try {
        const data = await apiGet("/api/options");
        
        fillDatalist("dlStatus", sortSmart(data.status));
        fillDatalist("dlOrigem", sortSmart(data.origem));

        if (tsCurso) {
            tsCurso.clearOptions();
            sortSmart(data.curso).forEach(c => tsCurso.addOption({ value: c, text: c }));
        }
        if (tsPolo) {
            tsPolo.clearOptions();
            sortSmart(data.polo).forEach(p => tsPolo.addOption({ value: p, text: p }));
        }
    } catch (e) {
        console.error("Erro ao carregar opções:", e);
    }
}

function fillDatalist(id, values) {
    const dl = document.getElementById(id);
    if (!dl) return;
    dl.innerHTML = "";
    values.forEach(v => {
        const opt = document.createElement("option");
        opt.value = v;
        dl.appendChild(opt);
    });
}

async function loadLeadsAndKpis() {
    showToast("Consultando BigQuery...", "ok");
    const params = getFilters();

    try {
        const [leads, kpis] = await Promise.all([
            apiGet("/api/leads", params),
            apiGet("/api/kpis", params),
        ]);

        renderTable(leads || []); // leads já deve vir como lista do seu backend
        renderKpis(kpis);

        showToast(`${Array.isArray(leads) ? leads.length : 0} registros carregados.`, "ok");
    } catch (e) {
        showToast(`Erro: ${e.message}`, "err");
    }
}

const loadLeadsAndKpisDebounced = (function(fn, delay) {
    let timeout;
    return (...args) => {
        clearTimeout(timeout);
        timeout = setTimeout(() => fn(...args), delay);
    };
})(loadLeadsAndKpis, 400);

/* ============================================================
   Renderização
============================================================ */
function renderTable(rows) {
    const tbody = $("#tbl tbody");
    if (!tbody) return;
    
    if (!rows || rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="10" style="text-align:center; padding:40px;">Nenhum lead encontrado para estes filtros.</td></tr>';
        return;
    }

    tbody.innerHTML = rows.map(r => `
        <tr>
            <td>${escapeHtml(fmtDate(r.data_inscricao))}</td>
            <td style="font-weight:600">${escapeHtml(r.nome)}</td>
            <td>${escapeHtml(r.cpf)}</td>
            <td>${escapeHtml(r.celular)}</td>
            <td>${escapeHtml(r.email)}</td>
            <td><span class="muted">${escapeHtml(r.origem)}</span></td>
            <td>${escapeHtml(r.polo)}</td>
            <td>${escapeHtml(r.curso)}</td>
            <td><strong>${escapeHtml(r.status)}</strong></td>
            <td>${escapeHtml(r.consultor)}</td>
        </tr>
    `).join("");
}

function renderKpis(k) {
    if ($("#kpiCount")) $("#kpiCount").textContent = k?.total || 0;
    if ($("#kpiTopStatus")) {
        const top = k?.top_status;
        $("#kpiTopStatus").textContent = top ? `${top.status} (${top.cnt})` : "-";
    }
    if ($("#kpiLastDate")) $("#kpiLastDate").textContent = fmtDate(k?.last_date);
}

/* ============================================================
   Event Bindings
============================================================ */
function bindActions() {
    $("#btnApply")?.addEventListener("click", (e) => {
        e.preventDefault();
        loadLeadsAndKpis();
    });

    $("#btnClear")?.addEventListener("click", (e) => {
        e.preventDefault();
        $("#fStatus").value = "";
        $("#fOrigem").value = "";
        $("#fIni").value = "";
        $("#fFim").value = "";
        if (tsCurso) tsCurso.clear();
        if (tsPolo) tsPolo.clear();
        loadLeadsAndKpis();
    });

    $("#btnReload")?.addEventListener("click", (e) => {
        e.preventDefault();
        loadOptions().then(loadLeadsAndKpis);
    });

    $("#btnUpload")?.addEventListener("click", async (e) => {
        e.preventDefault();
        const fileInput = $("#uploadFile");
        if (!fileInput.files.length) return alert("Selecione um arquivo.");
        
        const formData = new FormData();
        formData.append("file", fileInput.files[0]);
        formData.append("source", $("#uploadSource")?.value || "");

        setUploadStatus("Processando no BigQuery...", "muted");
        try {
            const res = await fetch("/api/upload", { method: "POST", body: formData });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error);
            setUploadStatus(`Sucesso! ${data.rows_loaded || 0} linhas processadas.`, "ok");
            await loadOptions();
            await loadLeadsAndKpis();
        } catch (e) {
            setUploadStatus(e.message, "error");
        }
    });
}

/* ============================================================
   Início da Aplicação
============================================================ */
document.addEventListener("DOMContentLoaded", async () => {
    // 1. Prepara a UI
    initMultiSelects();
    bindActions();
    
    // 2. Carrega as opções (Curso/Polo) para os selects
    await loadOptions();
    
    // 3. Busca os dados iniciais
    await loadLeadsAndKpis();
});
