const $ = (sel) => document.querySelector(sel);
let tsCurso, tsPolo;

/* Helpers */
function showToast(msg, type = "ok") {
    const statusLine = $("#statusLine");
    if (statusLine) {
        statusLine.textContent = msg;
        statusLine.className = type === "err" ? "error" : "";
    }
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
    return /^\d{4}-\d{2}-\d{2}/.test(s) ? s.slice(0, 10) : s.slice(0, 10);
}

/* API */
async function apiGet(path, params = {}) {
    const url = new URL(path, window.location.origin);
    Object.entries(params).forEach(([k, v]) => {
        if (!v || (Array.isArray(v) && v.length === 0)) return;
        url.searchParams.set(k, Array.isArray(v) ? v.join(" || ") : v);
    });
    const res = await fetch(url.toString(), { cache: "no-store" });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "Erro na API");
    return data;
}

/* Inicialização TomSelect */
function initMultiSelects() {
    const config = {
        plugins: ['remove_button', 'checkbox_options'],
        maxItems: null,
        valueField: 'value',
        labelField: 'text',
        searchField: ['text'],
        render: {
            option: function(data, escape) {
                return `<div class="d-flex-option"><span class="checkbox"></span><span>${escape(data.text)}</span></div>`;
            }
        },
        onChange: () => loadLeadsAndKpisDebounced()
    };

    tsCurso = new TomSelect("#fCurso", config);
    tsPolo = new TomSelect("#fPolo", config);
}

async function loadOptions() {
    try {
        const data = await apiGet("/api/options");
        if (tsCurso) {
            tsCurso.clearOptions();
            data.curso.forEach(c => tsCurso.addOption({ value: c, text: c }));
        }
        if (tsPolo) {
            tsPolo.clearOptions();
            data.polo.forEach(p => tsPolo.addOption({ value: p, text: p }));
        }
        // Preencher datalists simples
        const fillDL = (id, vals) => {
            const el = document.getElementById(id);
            if (el) el.innerHTML = vals.map(v => `<option value="${v}">`).join("");
        };
        fillDL("dlStatus", data.status);
        fillDL("dlOrigem", data.origem);
    } catch (e) { console.error(e); }
}

async function loadLeadsAndKpis() {
    showToast("A consultar BigQuery...", "ok");
    const params = {
        status: $("#fStatus")?.value,
        curso: tsCurso?.getValue(),
        polo: tsPolo?.getValue(),
        origem: $("#fOrigem")?.value,
        data_ini: $("#fIni")?.value,
        data_fim: $("#fFim")?.value,
        limit: $("#fLimit")?.value
    };

    try {
        const [leads, kpis] = await Promise.all([
            apiGet("/api/leads", params),
            apiGet("/api/kpis", params)
        ]);
        renderTable(leads);
        renderKpis(kpis);
        showToast(`${leads.length} registos encontrados.`, "ok");
    } catch (e) { showToast(e.message, "err"); }
}

const loadLeadsAndKpisDebounced = (() => {
    let t; return () => { clearTimeout(t); t = setTimeout(loadLeadsAndKpis, 400); };
})();

function renderTable(rows) {
    const tbody = $("#tbl tbody");
    if (!tbody) return;
    tbody.innerHTML = rows.map(r => `
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
        </tr>`).join("");
}

function renderKpis(k) {
    if ($("#kpiCount")) $("#kpiCount").textContent = k?.total || 0;
    if ($("#kpiTopStatus")) $("#kpiTopStatus").textContent = k?.top_status ? `${k.top_status.status} (${k.top_status.cnt})` : "-";
}

document.addEventListener("DOMContentLoaded", async () => {
    initMultiSelects();
    $("#btnApply")?.addEventListener("click", loadLeadsAndKpis);
    $("#btnClear")?.addEventListener("click", () => {
        window.location.reload(); // Forma mais segura de limpar tudo
    });
    await loadOptions();
    await loadLeadsAndKpis();
});
