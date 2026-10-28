/* Search and kind filters for the Exchange catalog. The catalog is plain HTML that
   lists every item, so without this script everything still shows; with it, every
   whitespace-separated term must appear in an item's name, description, author,
   kind or tags, and the kind buttons narrow to the kinds pressed. */

(function () {
  "use strict";

  function setUp(root) {
    var input = root.querySelector(".exchange-catalog__search");
    var buttons = Array.prototype.slice.call(root.querySelectorAll(".exchange-catalog__kind"));
    var cards = Array.prototype.slice.call(root.querySelectorAll(".exchange-card"));
    var count = root.querySelector(".exchange-catalog__count");
    var empty = root.querySelector(".exchange-catalog__empty");

    function apply() {
      var terms = input.value.toLowerCase().split(/\s+/).filter(Boolean);
      var kinds = buttons
        .filter(function (b) { return b.getAttribute("aria-pressed") === "true"; })
        .map(function (b) { return b.dataset.kind; });
      var shown = 0;
      cards.forEach(function (card) {
        var visible = (!kinds.length || kinds.indexOf(card.dataset.kind) !== -1) &&
          terms.every(function (t) { return card.dataset.text.indexOf(t) !== -1; });
        card.hidden = !visible;
        if (visible) shown++;
      });
      count.textContent = shown === cards.length ? cards.length + " items" : shown + " of " + cards.length + " items";
      empty.hidden = shown > 0;
    }

    input.addEventListener("input", apply);
    buttons.forEach(function (b) {
      b.addEventListener("click", function () {
        b.setAttribute("aria-pressed", b.getAttribute("aria-pressed") === "true" ? "false" : "true");
        apply();
      });
    });
    // A search link such as catalog/?q=rmm opens already filtered.
    var q = new URLSearchParams(window.location.search).get("q");
    if (q) input.value = q;
    apply();
  }

  function init() {
    var root = document.querySelector(".exchange-catalog");
    if (root && !root.dataset.ready) {
      root.dataset.ready = "1";
      setUp(root);
    }
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
