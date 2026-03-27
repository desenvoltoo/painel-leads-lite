// static/js/login.js - autenticação da rota /login
const loginUserEl = document.getElementById("loginUser");
const loginPassEl = document.getElementById("loginPass");
const loginStatusEl = document.getElementById("loginStatus");

async function doLogin() {
  const username = (loginUserEl?.value || "").trim();
  const password = (loginPassEl?.value || "").trim();

  if (loginStatusEl) {
    loginStatusEl.textContent = "Autenticando...";
    loginStatusEl.className = "muted";
  }

  try {
    const res = await fetch("/api/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data?.ok) {
      throw new Error(data?.error || "Falha no login.");
    }

    if (loginStatusEl) {
      loginStatusEl.textContent = "Login realizado com sucesso!";
      loginStatusEl.className = "status-ok";
    }

    window.location.href = "/";
  } catch (err) {
    if (loginStatusEl) {
      loginStatusEl.textContent = err?.message || "Erro de autenticação.";
      loginStatusEl.className = "error";
    }
  }
}

document.getElementById("btnLogin")?.addEventListener("click", doLogin);
loginPassEl?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") doLogin();
});
