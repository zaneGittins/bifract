/* Free-text search over the operations in the API reference.
   Swagger UI's own filter box matches tag names only, and the reference is an
   iframe, so the site search index does not reach any of the operations either.
   This reads the spec Swagger UI already loaded, searches method, path, summary
   and tag, and opens the chosen operation in the iframe below. */

(function () {
  "use strict";

  var METHODS = ["get", "post", "put", "delete", "patch", "head", "options"];
  var MAX_RESULTS = 40;
  var READY_TIMEOUT_MS = 20000;
  var POLL_MS = 100;

  function operations(spec) {
    var out = [];
    var paths = (spec && spec.paths) || {};
    Object.keys(paths).forEach(function (path) {
      var item = paths[path] || {};
      METHODS.forEach(function (method) {
        var op = item[method];
        if (!op) return;
        var tag = (op.tags || [])[0] || "default";
        out.push({
          method: method,
          path: path,
          summary: op.summary || "",
          tag: tag,
          id: op.operationId || "",
          route: (method + " " + path).toLowerCase(),
          haystack: (method + " " + path + " " + op.summary + " " + tag).toLowerCase(),
        });
      });
    });
    return out;
  }

  // Every whitespace-separated term must appear, so "post alert" narrows rather
  // than widening the way a single substring match would. Terms that land on the
  // method or path identify an operation more strongly than ones that only hit
  // its prose, and among equals the shorter path is the more canonical route.
  function match(ops, query) {
    var terms = query.toLowerCase().split(/\s+/).filter(Boolean);
    if (!terms.length) return [];
    var hits = [];
    ops.forEach(function (op) {
      var score = 0;
      for (var i = 0; i < terms.length; i++) {
        if (op.haystack.indexOf(terms[i]) === -1) return;
        if (op.route.indexOf(terms[i]) !== -1) score++;
      }
      hits.push({ op: op, score: score });
    });
    hits.sort(function (a, b) {
      return b.score - a.score || a.op.path.length - b.op.path.length;
    });
    return hits.map(function (hit) {
      return hit.op;
    });
  }

  // Mirrors Swagger UI's escapeDeepLinkPath, which is what it names the
  // operation's element with. Reproduced rather than reached for because the
  // iframe does not export it.
  function elementId(op) {
    var raw = "operations-" + op.tag + "-" + op.id;
    raw = raw.trim().replace(/\s/g, "_");
    return window.CSS && window.CSS.escape ? window.CSS.escape(raw) : raw;
  }

  // Expands the operation inside the iframe and scrolls the page to it. Uses
  // Swagger UI's own layout actions, the same ones deep links drive; if they
  // ever move, the result list still answers "which endpoint is it".
  function reveal(iframe, op) {
    var win = iframe.contentWindow;
    try {
      var actions = win.ui.getSystem().layoutActions;
      actions.show(["operations-tag", op.tag], true);
      actions.show(["operations", op.tag, op.id], true);
    } catch (e) {
      return;
    }
    window.setTimeout(function () {
      var el = win.document.getElementById(elementId(op));
      if (!el) return;
      var top = iframe.getBoundingClientRect().top + window.scrollY + el.offsetTop;
      window.scrollTo({ top: Math.max(0, top - 80), behavior: "smooth" });
    }, 150);
  }

  function render(list, results, total, iframe) {
    list.textContent = "";
    if (!results.length) {
      list.hidden = false;
      var empty = document.createElement("p");
      empty.className = "api-search__empty";
      empty.textContent = "No operation matches that.";
      list.appendChild(empty);
      return;
    }

    results.forEach(function (op) {
      var row = document.createElement("button");
      row.type = "button";
      row.className = "api-search__hit";
      row.addEventListener("click", function () {
        reveal(iframe, op);
      });

      var method = document.createElement("span");
      method.className = "api-search__method api-search__method--" + op.method;
      method.textContent = op.method.toUpperCase();

      var path = document.createElement("code");
      path.className = "api-search__path";
      path.textContent = op.path;

      row.appendChild(method);
      row.appendChild(path);

      if (op.summary) {
        var summary = document.createElement("span");
        summary.className = "api-search__summary";
        summary.textContent = op.summary;
        row.appendChild(summary);
      }
      list.appendChild(row);
    });

    if (total > results.length) {
      var more = document.createElement("p");
      more.className = "api-search__empty";
      more.textContent =
        "Showing " + results.length + " of " + total + " matches. Add another word to narrow it.";
      list.appendChild(more);
    }
    list.hidden = false;
  }

  function mount(iframe, ops) {
    var panel = document.createElement("div");
    panel.className = "api-search";

    var input = document.createElement("input");
    input.type = "search";
    input.className = "api-search__input";
    input.setAttribute("aria-label", "Search operations");
    input.placeholder = "Search " + ops.length + " operations by path, summary or method";

    var list = document.createElement("div");
    list.className = "api-search__results";
    list.hidden = true;

    panel.appendChild(input);
    panel.appendChild(list);
    iframe.parentNode.insertBefore(panel, iframe);

    var current = [];
    input.addEventListener("input", function () {
      var query = input.value.trim();
      if (!query) {
        current = [];
        list.hidden = true;
        list.textContent = "";
        return;
      }
      var found = match(ops, query);
      current = found.slice(0, MAX_RESULTS);
      render(list, current, found.length, iframe);
    });

    input.addEventListener("keydown", function (event) {
      if (event.key === "Enter" && current.length) {
        event.preventDefault();
        reveal(iframe, current[0]);
      } else if (event.key === "Escape") {
        input.value = "";
        input.dispatchEvent(new Event("input"));
      }
    });
  }

  // The spec is fetched by Swagger UI after its own load event, so the iframe
  // being ready is not the same as the operations being known.
  function whenLoaded(iframe, done) {
    var waited = 0;
    var timer = window.setInterval(function () {
      var spec = null;
      try {
        spec = iframe.contentWindow.ui.specSelectors.specJson().toJS();
      } catch (e) {
        spec = null;
      }
      if (spec && spec.paths && Object.keys(spec.paths).length) {
        window.clearInterval(timer);
        done(spec);
        return;
      }
      waited += POLL_MS;
      if (waited >= READY_TIMEOUT_MS) window.clearInterval(timer);
    }, POLL_MS);
  }

  document.addEventListener("DOMContentLoaded", function () {
    var iframe = document.querySelector("iframe.swagger-ui-iframe");
    if (!iframe) return;
    whenLoaded(iframe, function (spec) {
      var ops = operations(spec);
      if (ops.length) mount(iframe, ops);
    });
  });
})();
