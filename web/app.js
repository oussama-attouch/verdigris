// Verdigris admin UI — one ES module, no build step, no framework.
//
// The whole file is: a small API wrapper, one piece of module-scoped state, one
// pure render function, and the event wiring. Everything it draws comes from
// the server, so every string it touches is treated as text and never as markup
// — see renderRow, which is the only place a key or a value reaches the DOM.

const API = {
  health: () => fetch("/api/health").then((r) => r.json()),
  dump: () => fetch("/api/dump").then((r) => r.json()),
  set: (key, value, ttl) =>
    fetch("/api/set", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ key, value, ttl }),
    }).then((r) => r.json()),
  del: (key) =>
    fetch(`/api/del?key=${encodeURIComponent(key)}`, { method: "DELETE" }).then((r) => r.json()),
  save: () => fetch("/api/save", { method: "POST" }).then((r) => r.json()),
};

// The auto-refresh cadence. Five seconds is slow enough that a person editing a
// value is not fighting the table, and fast enough to see another client's
// writes appear.
const REFRESH_MS = 5000;

// How long the "Saved" toast stays up.
const TOAST_MS = 2000;

const DISCONNECTED_MESSAGE =
  "Verdigris is not reachable. Start it with verdigris -port 6379.";

// Every element the module needs, looked up once. A typo in an id fails loudly
// here at load rather than silently doing nothing on a click.
const el = {
  statusPill: document.getElementById("status-pill"),
  statusText: document.getElementById("status-text"),
  refreshButton: document.getElementById("refresh-button"),
  saveButton: document.getElementById("save-button"),
  autoRefresh: document.getElementById("auto-refresh"),
  errorBanner: document.getElementById("error-banner"),
  form: document.getElementById("key-form"),
  formTitle: document.getElementById("form-title"),
  keyInput: document.getElementById("key-input"),
  valueInput: document.getElementById("value-input"),
  ttlInput: document.getElementById("ttl-input"),
  submitButton: document.getElementById("submit-button"),
  cancelButton: document.getElementById("cancel-button"),
  table: document.getElementById("keys-table"),
  tbody: document.getElementById("keys-tbody"),
  emptyState: document.getElementById("empty-state"),
  toast: document.getElementById("toast"),
};

// state is the whole of what the UI remembers between renders.
//
//   editingKey  the key the form is currently editing, or null when adding
//   autoRefresh whether the checkbox is ticked — the user's intent
//   paused      set when a fetch failed; suspends the timer without
//               touching autoRefresh, so the checkbox keeps showing what the
//               user asked for and a later manual Refresh resumes it
const state = {
  editingKey: null,
  autoRefresh: true,
  paused: false,
};

let refreshTimer = null;
let toastTimer = null;

// lastRows is the data the table currently shows. The row buttons are wired
// through event delegation to an index into this, so a click resolves to the
// same object the row was drawn from — including when a key contains characters
// that would break a CSS selector.
let lastRows = [];

// --- rendering ------------------------------------------------------------

// ttlLabel renders a TTL cell.
//
// The value is a point-in-time observation from the last refresh, so it is
// shown as one: no countdown, because a number ticking down between refreshes
// would be inventing data the server never sent, and it would be wrong for any
// key whose expiry another client changed.
function ttlLabel(ttl) {
  if (ttl === -1) return { text: "—", gone: false }; // exists, no expiry
  if (ttl === -2) return { text: "gone", gone: true }; // no such key
  return { text: `${ttl}s`, gone: false };
}

// renderRow builds one table row.
//
// This is the only place a key or a value from the server reaches the document,
// and both go in through textContent. Assigning them to innerHTML would make
// every value in the keyspace a script injection: a client can store
// `<script>…</script>` under any name, and the admin UI is where an operator
// would go to look at it.
function renderRow(row, index) {
  const tr = document.createElement("tr");
  tr.className = "keys-table__row";
  if (state.editingKey === row.key) {
    tr.classList.add("keys-table__row--editing");
  }

  const keyCell = document.createElement("td");
  keyCell.className = "keys-table__cell--key";
  // textContent, not innerHTML: a key is arbitrary bytes from any client.
  keyCell.textContent = row.key;

  const valueCell = document.createElement("td");
  valueCell.className = "keys-table__cell--value";
  if (row.exists && row.value !== null) {
    valueCell.textContent = row.value;
  } else {
    // A key that vanished between the dump's KEYS and its GET. Shown as an
    // explicit note rather than an empty cell, which would read as "this key
    // holds the empty string".
    valueCell.textContent = "(gone)";
    valueCell.classList.add("ttl--gone");
  }

  const ttlCell = document.createElement("td");
  ttlCell.className = "keys-table__cell--ttl";
  const ttl = ttlLabel(row.ttl);
  ttlCell.textContent = ttl.text;
  if (ttl.gone) {
    ttlCell.classList.add("ttl--gone");
  }

  const actionsCell = document.createElement("td");
  actionsCell.className = "keys-table__cell--actions";

  const editButton = document.createElement("button");
  editButton.type = "button";
  editButton.className = "button button--small";
  editButton.textContent = "Edit";
  // The row is identified by its index, not by its key: see handleRowAction.
  editButton.dataset.action = "edit";
  editButton.dataset.index = String(index);

  const deleteButton = document.createElement("button");
  deleteButton.type = "button";
  deleteButton.className = "button button--small button--danger";
  deleteButton.textContent = "Delete";
  deleteButton.dataset.action = "delete";
  deleteButton.dataset.index = String(index);

  actionsCell.append(editButton, deleteButton);
  tr.append(keyCell, valueCell, ttlCell, actionsCell);

  return tr;
}

// render is a pure function from (state, rows) to the DOM: it reads nothing
// else and fetches nothing. That is what makes the rest of the file easy to
// reason about — everything that can change the page is a render call.
function render(rows) {
  lastRows = rows;

  // rebuild, not patch. The keyspace is small and a full replace cannot drift
  // out of sync with the data the way an incremental update can.
  el.tbody.replaceChildren();

  rows.forEach((row, index) => {
    el.tbody.appendChild(renderRow(row, index));
  });

  const empty = rows.length === 0;
  el.emptyState.hidden = !empty;
  el.table.hidden = empty;

  renderForm();
}

// renderForm reflects edit mode in the form.
function renderForm() {
  const editing = state.editingKey !== null;

  el.formTitle.textContent = editing ? `Editing ${state.editingKey}` : "Add a key";
  el.submitButton.textContent = editing ? "Update" : "Set";
  el.cancelButton.hidden = !editing;

  // While editing, the key identifies the row being changed, so letting it be
  // retyped would silently turn an update into a write to a different key.
  el.keyInput.readOnly = editing;
}

// setStatus updates the connection pill. It is called on every refresh, so the
// pill can never lag behind the data on screen.
function setStatus(connected) {
  el.statusPill.classList.toggle("pill--connected", connected);
  el.statusPill.classList.toggle("pill--disconnected", !connected);
  el.statusPill.classList.toggle("pill--unknown", false);
  el.statusText.textContent = connected ? "Connected" : "Disconnected";
}

function showBanner(message) {
  el.errorBanner.textContent = message;
  el.errorBanner.hidden = false;
}

function hideBanner() {
  el.errorBanner.hidden = true;
}

function showToast(message) {
  el.toast.textContent = message;
  el.toast.hidden = false;

  // Restarted on every call, so a second Save does not inherit the first
  // toast's remaining time and vanish early.
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    el.toast.hidden = true;
  }, TOAST_MS);
}

// --- data -----------------------------------------------------------------

// refresh fetches health and the keyspace together and renders the result.
//
// Promise.all rather than two awaits: the two requests are independent, and
// running them in sequence would make every refresh take the sum of both round
// trips for no reason.
async function refresh() {
  let health;
  let rows;

  try {
    [health, rows] = await Promise.all([API.health(), API.dump()]);
  } catch (err) {
    // The UI itself could not reach its own backend, which is a different
    // failure from Verdigris being down and is reported as such.
    onFetchFailure(`Cannot reach the UI server: ${err.message}`);
    return;
  }

  // /api/dump answers 502 with {"error": ...} when Verdigris is unreachable, so
  // an error body is not an array and must not be rendered as one.
  if (!Array.isArray(rows)) {
    onFetchFailure(typeof rows?.error === "string" ? rows.error : DISCONNECTED_MESSAGE);
    return;
  }

  const connected = health?.verdigris === "up";
  setStatus(connected);

  if (connected) {
    hideBanner();
  } else {
    showBanner(health?.error ? `${DISCONNECTED_MESSAGE} (${health.error})` : DISCONNECTED_MESSAGE);
  }

  render(rows);
}

// onFetchFailure handles any failure to get data: it shows the banner, marks
// the connection dead, and pauses the timer so a broken backend is not polled
// every five seconds forever.
function onFetchFailure(message) {
  setStatus(false);
  showBanner(message);
  pauseAutoRefresh();
}

// pauseAutoRefresh stops the timer without changing the checkbox, so a manual
// Refresh can resume exactly what the user asked for.
function pauseAutoRefresh() {
  state.paused = true;
  stopTimer();
}

// --- auto-refresh ---------------------------------------------------------

function stopTimer() {
  if (refreshTimer !== null) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

// startTimer starts the interval if it should be running: the user wants it and
// nothing has failed since the last successful refresh.
function startTimer() {
  stopTimer();
  if (!state.autoRefresh || state.paused) return;
  refreshTimer = setInterval(refresh, REFRESH_MS);
}

// --- actions --------------------------------------------------------------

async function handleSubmit(event) {
  event.preventDefault();

  const key = el.keyInput.value;
  const value = el.valueInput.value;

  if (key === "") {
    showBanner("A key is required.");
    return;
  }

  // An empty TTL field means "no expiry", which is a different request from a
  // TTL of zero — the latter is refused by the API rather than silently meaning
  // something else.
  const rawTTL = el.ttlInput.value.trim();
  const ttl = rawTTL === "" ? null : Number(rawTTL);
  if (ttl !== null && (!Number.isInteger(ttl) || ttl <= 0)) {
    showBanner("TTL must be a whole number of seconds greater than zero.");
    return;
  }

  let reply;
  try {
    // Editing sends the same key it is editing. Verdigris overwrites, so an
    // update is just a set — there is no separate endpoint to keep in step.
    reply = await API.set(key, value, ttl);
  } catch (err) {
    onFetchFailure(`Cannot reach the UI server: ${err.message}`);
    return;
  }

  if (reply?.ok !== true) {
    onFetchFailure(reply?.error ?? "The set failed.");
    return;
  }

  resetForm();
  await refresh();
  startTimer();
}

// resetForm returns the form to add mode and clears it.
function resetForm() {
  state.editingKey = null;
  el.form.reset();
  renderForm();
}

function startEditing(key, value) {
  state.editingKey = key;
  el.keyInput.value = key;
  el.valueInput.value = value;
  // The TTL is deliberately left blank. The dump reports the time left, not the
  // timeout that was set, so pre-filling it would write back a smaller TTL than
  // the key started with — the value would quietly count down on every save.
  el.ttlInput.value = "";
  renderForm();
  el.valueInput.focus();
}

async function handleRowAction(action, index) {
  // The row is identified by its index into the last dump rather than by its
  // key. A key is arbitrary bytes from any client, and using it as a selector
  // value would need quoting rules that get it wrong for a key containing a
  // quote or a bracket — silently, and only for that key.
  const row = lastRows[index];
  if (row === undefined) {
    // The table was rebuilt between the click and the handler, so the row being
    // acted on is gone. Re-rendering is the honest response.
    await refresh();
    return;
  }

  if (action === "edit") {
    // The value comes from the data the table was rendered from, not from the
    // cell's text, so what the form fills with is exactly what the server sent
    // rather than a rendering of it.
    const value = row.exists && row.value !== null ? row.value : "";
    startEditing(row.key, value);
    return;
  }

  if (action === "delete") {
    // confirm() is synchronous and blocks the page, which is the point: this
    // deletes data and there is no undo.
    if (!confirm(`Delete "${row.key}"?`)) return;

    let reply;
    try {
      reply = await API.del(row.key);
    } catch (err) {
      onFetchFailure(`Cannot reach the UI server: ${err.message}`);
      return;
    }

    if (reply?.error) {
      onFetchFailure(reply.error);
      return;
    }

    // Deleting the key the form is editing leaves the form pointing at
    // something that no longer exists, so it goes back to add mode.
    if (state.editingKey === row.key) {
      resetForm();
    }

    await refresh();
    startTimer();
  }
}

async function handleSave() {
  let reply;
  try {
    reply = await API.save();
  } catch (err) {
    onFetchFailure(`Cannot reach the UI server: ${err.message}`);
    return;
  }

  if (reply?.ok !== true) {
    onFetchFailure(reply?.error ?? "The save failed.");
    return;
  }

  showToast("Saved");
}

// --- wiring ---------------------------------------------------------------

el.refreshButton.addEventListener("click", async () => {
  // A manual refresh is the user saying "try again", so it clears the pause and
  // restarts the timer if it was running before.
  state.paused = false;
  await refresh();
  // A refresh that failed has set state.paused again, so this only restarts the
  // timer when the backend actually answered.
  startTimer();
});

el.saveButton.addEventListener("click", handleSave);

el.autoRefresh.addEventListener("change", () => {
  state.autoRefresh = el.autoRefresh.checked;
  if (state.autoRefresh) {
    // Ticking the box is also a request to try again.
    state.paused = false;
    refresh().then(startTimer);
  } else {
    stopTimer();
  }
});

el.form.addEventListener("submit", handleSubmit);
el.cancelButton.addEventListener("click", resetForm);

// One listener on the tbody, not two per row: rows are rebuilt on every
// refresh, and per-row listeners would be re-attached five seconds apart
// forever, leaking the old ones.
el.tbody.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-action]");
  if (button === null) return;
  handleRowAction(button.dataset.action, Number(button.dataset.index));
});

// Leaving the page should not leave a timer behind.
window.addEventListener("beforeunload", stopTimer);

// --- start ----------------------------------------------------------------

// The initial state is "connecting": the first refresh decides whether that
// becomes Connected or Disconnected. It runs immediately rather than waiting
// out the first interval, so the table is never blank for five seconds.
setStatus(false);
refresh().then(startTimer);
