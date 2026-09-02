# Example alert policies

Policy rules assert things about an alert's definition: that it carries an ATT&CK label,
that its description is long enough to act on, that it has a test proving what it should
ignore. They run as an analyst types in the alert editor and again when the alert is
saved, so a `block` rule cannot be bypassed by an API client.

A rule is `field`, `operator`, `value`, `message`, `severity`. There is no scripting
language: the checks a CI job would write in bash reduce to that shape, and keeping them
as data means they can run on every keystroke.

`message` is the part that matters. It is what an analyst reads next to the field they
got wrong, so write what to do ("Add an ATT&CK technique label, for example
attack.t1059") rather than what failed.

## Using one

Alerts -> Policies -> Import, then pick a file here. Check the compliance view before
promoting anything to `block`: existing alerts keep running whatever the rules say, but
the next person to edit one has to satisfy them first.

Feed-managed alerts are exempt throughout, since their definitions come from upstream.

## Files

- `soc-baseline.yaml` - a conservative starting set: ATT&CK labels and a non-wildcard
  query block, everything else advises.
