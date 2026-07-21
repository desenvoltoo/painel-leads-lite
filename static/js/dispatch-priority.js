// Realce operacional da fila de disparos.
(() => {
  "use strict";

  const TABLE_SELECTOR = "#tbl";
  const DATA_DISPARO_COLUMN_INDEX = 11;

  function ensurePriorityNote() {
    const table = document.querySelector(TABLE_SELECTOR);
    const card = table?.closest("section.card");
    if (!table || !card || card.querySelector(".dispatch-priority-note")) return;

    const note = document.createElement("div");
    note.className = "dispatch-priority-note";
    note.innerHTML = `
      <div class="dispatch-priority-note__copy">
        <div class="dispatch-priority-note__icon" aria-hidden="true">⚡</div>
        <div>
          <strong>Fila priorizada para disparo</strong>
          <p>Leads sem data de disparo aparecem primeiro. Dentro da fila, inscrições mais recentes têm prioridade.</p>
        </div>
      </div>
      <div class="dispatch-priority-counter" aria-live="polite">
        <span>Prontos nesta página</span>
        <strong id="dispatchReadyCount">0</strong>
      </div>
    `;

    const header = card.querySelector(".card-header");
    if (header?.nextSibling) {
      card.insertBefore(note, header.nextSibling);
    } else {
      card.insertBefore(note, table.parentElement);
    }
  }

  function decorateRows() {
    const tbody = document.querySelector(`${TABLE_SELECTOR} tbody`);
    if (!tbody) return;

    let ready = 0;
    tbody.querySelectorAll("tr").forEach((row) => {
      const cells = row.querySelectorAll("td");
      if (cells.length <= DATA_DISPARO_COLUMN_INDEX) return;

      const dispatchCell = cells[DATA_DISPARO_COLUMN_INDEX];
      const text = (dispatchCell.textContent || "").trim().toLowerCase();
      const isPending = !text || text === "-" || text.includes("sem disparo") || text.includes("pronto para disparo");

      row.classList.toggle("lead-pendente-disparo", isPending);
      row.classList.toggle("lead-ja-disparado", !isPending);

      if (isPending) {
        ready += 1;
        if (!dispatchCell.querySelector(".dispatch-ready-badge")) {
          dispatchCell.textContent = "";
          const badge = document.createElement("span");
          badge.className = "dispatch-ready-badge";
          badge.textContent = "Pronto para disparo";
          dispatchCell.appendChild(badge);
        }
      }
    });

    const counter = document.querySelector("#dispatchReadyCount");
    if (counter) counter.textContent = String(ready);
  }

  function updateCopy() {
    const title = document.querySelector("#dashboardTitle");
    if (title) title.textContent = "Central de Disparos";

    const subtitle = document.querySelector(".dashboard-hero p");
    if (subtitle) {
      subtitle.textContent = "Priorize bases ainda não disparadas, trabalhe os cadastros mais recentes e acompanhe toda a operação em uma única fila.";
    }

    const listingTitle = [...document.querySelectorAll("section.card h2")]
      .find((node) => (node.textContent || "").toLowerCase().includes("leads consolidados"));
    if (listingTitle) listingTitle.textContent = "Fila de leads para disparo";
  }

  function initialize() {
    ensurePriorityNote();
    updateCopy();
    decorateRows();

    const tbody = document.querySelector(`${TABLE_SELECTOR} tbody`);
    if (!tbody) return;

    const observer = new MutationObserver(() => decorateRows());
    observer.observe(tbody, { childList: true, subtree: true, characterData: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initialize, { once: true });
  } else {
    initialize();
  }
})();
