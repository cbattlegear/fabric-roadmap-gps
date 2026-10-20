const form = document.querySelector(".insights-filters");
if (form) {
  form.addEventListener("submit", () => {
    form.querySelector("button").disabled = true;
    document.getElementById("insights-loading").hidden = false;
  });
  window.addEventListener("pageshow", () => {
    form.querySelector("button").disabled = false;
    document.getElementById("insights-loading").hidden = true;
  });
}

function revealEvidence() {
  const panel = document.getElementById("evidence");
  if (panel && window.location.hash === "#evidence") {
    panel.open = true;
    panel.scrollIntoView({ block: "start" });
  }
}
window.addEventListener("hashchange", revealEvidence);
revealEvidence();
