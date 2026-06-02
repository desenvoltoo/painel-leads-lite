(function () {
  "use strict";

  const dataNode = document.getElementById("gestao-data");
  if (!dataNode) return;

  const payload = JSON.parse(dataNode.textContent || "{}");
  const operacao = payload.operacao || {};
  const produtividade = payload.produtividade || [];
  const fila = payload.fila_operacional || [];
  const alertas = payload.alertas || {};

  const numberFmt = new Intl.NumberFormat("pt-BR");
  const decimalFmt = new Intl.NumberFormat("pt-BR", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const pctFmt = new Intl.NumberFormat("pt-BR", { minimumFractionDigits: 1, maximumFractionDigits: 1 });

  function num(value) {
    const parsed = Number(value || 0);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function fmtNumber(value) { return numberFmt.format(num(value)); }
  function fmtDecimal(value) { return decimalFmt.format(num(value)); }
  function fmtPct(value) { return `${pctFmt.format(num(value))}%`; }

  function fmtDate(value) {
    if (!value) return "-";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value).slice(0, 16);
    return date.toLocaleString("pt-BR", { dateStyle: "short", timeStyle: "short" });
  }

  function setText(selector, value) {
    document.querySelectorAll(selector).forEach((el) => { el.textContent = value; });
  }

  function hydrateKpis() {
    Object.entries(operacao).forEach(([key, value]) => {
      const formatted = key === "media_horas_primeiro_contato" ? `${fmtDecimal(value)}h` : fmtNumber(value);
      setText(`[data-kpi="${key}"]`, formatted);
    });
    Object.entries(alertas).forEach(([key, value]) => setText(`[data-alert="${key}"]`, fmtNumber(value)));
    const generatedAt = document.getElementById("generatedAt");
    if (generatedAt && payload.generated_at) generatedAt.textContent = fmtDate(payload.generated_at);
  }

  function priorityBadge(value) {
    const text = value || "-";
    const slug = String(text).toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/\s+/g, "-");
    return `<span class="priority-badge priority-${slug}">${text}</span>`;
  }

  function initProdutividadeTable() {
    $("#produtividadeTable").DataTable({
      data: produtividade,
      responsive: true,
      pageLength: 25,
      order: [[4, "desc"]],
      dom: "Bfrtip",
      buttons: [{ extend: "csvHtml5", text: "Exportar CSV", className: "btn btn-primary btn-sm", bom: true, filename: "produtividade_consultores" }],
      language: { url: "https://cdn.datatables.net/plug-ins/1.13.8/i18n/pt-BR.json" },
      columns: [
        { data: "consultor_comercial", defaultContent: "-" },
        { data: "total_leads", render: fmtNumber },
        { data: "leads_nao_disparados", render: fmtNumber },
        { data: "leads_disparados", render: fmtNumber },
        { data: "matriculados", render: fmtNumber },
        { data: "taxa_matricula_pct", render: fmtPct },
        { data: "ultima_atividade", render: fmtDate },
        { data: "score_medio_carteira", render: fmtDecimal },
        { data: "leads_criticos", render: fmtNumber },
        { data: "leads_sem_movimento_7_dias", render: fmtNumber },
      ],
    });
  }

  function initFilaTable() {
    $("#filaTable").DataTable({
      data: fila,
      responsive: true,
      pageLength: 25,
      order: [[6, "desc"]],
      dom: "frtip",
      language: { url: "https://cdn.datatables.net/plug-ins/1.13.8/i18n/pt-BR.json" },
      columns: [
        { data: "nome", defaultContent: "-" },
        { data: "celular", defaultContent: "-" },
        { data: "curso", defaultContent: "-" },
        { data: "polo", defaultContent: "-" },
        { data: "status", defaultContent: "-" },
        { data: "consultor_comercial", defaultContent: "-" },
        { data: "score_prioridade", render: fmtDecimal },
        { data: "nivel_prioridade", render: priorityBadge },
        { data: "etapa_operacional", defaultContent: "-" },
        { data: "dias_sem_acao", render: fmtNumber },
      ],
    });
  }

  function topBy(key, limit) {
    return [...produtividade]
      .sort((a, b) => num(b[key]) - num(a[key]))
      .slice(0, limit)
      .filter((row) => num(row[key]) > 0);
  }

  function initCharts() {
    const matriculasRows = topBy("matriculados", 10);
    const conversaoRows = topBy("taxa_matricula_pct", 10);
    const baseOptions = {
      indexAxis: "y",
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { beginAtZero: true, grid: { color: "rgba(148,163,184,.22)" } }, y: { grid: { display: false } } },
    };

    new Chart(document.getElementById("chartMatriculas"), {
      type: "bar",
      data: {
        labels: matriculasRows.map((row) => row.consultor_comercial || "Sem consultor"),
        datasets: [{ data: matriculasRows.map((row) => num(row.matriculados)), backgroundColor: "rgba(37, 99, 235, .82)", borderRadius: 10 }],
      },
      options: baseOptions,
    });

    new Chart(document.getElementById("chartConversao"), {
      type: "bar",
      data: {
        labels: conversaoRows.map((row) => row.consultor_comercial || "Sem consultor"),
        datasets: [{ data: conversaoRows.map((row) => num(row.taxa_matricula_pct)), backgroundColor: "rgba(22, 163, 74, .82)", borderRadius: 10 }],
      },
      options: { ...baseOptions, scales: { ...baseOptions.scales, x: { ...baseOptions.scales.x, ticks: { callback: (value) => `${value}%` } } } },
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    hydrateKpis();
    initCharts();
    initProdutividadeTable();
    initFilaTable();
  });
})();
